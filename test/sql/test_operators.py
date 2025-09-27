import collections.abc as collections_abc
import datetime
import enum
import operator
import pickle
import re

from sqlalchemy import all_
from sqlalchemy import and_
from sqlalchemy import any_
from sqlalchemy import asc
from sqlalchemy import between
from sqlalchemy import bindparam
from sqlalchemy import bitwise_not
from sqlalchemy import desc
from sqlalchemy import distinct
from sqlalchemy import Enum
from sqlalchemy import exc
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import LargeBinary
from sqlalchemy import literal_column
from sqlalchemy import not_
from sqlalchemy import nulls_first
from sqlalchemy import nulls_last
from sqlalchemy import Numeric
from sqlalchemy import or_
from sqlalchemy import SQLColumnExpression
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import type_coerce
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import oracle
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import default
from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from sqlalchemy.sql import coercions
from sqlalchemy.sql import collate
from sqlalchemy.sql import column
from sqlalchemy.sql import compiler
from sqlalchemy.sql import false
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql import literal
from sqlalchemy.sql import null
from sqlalchemy.sql import operators
from sqlalchemy.sql import roles
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql import table
from sqlalchemy.sql import true
from sqlalchemy.sql import values
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.elements import BooleanClauseList
from sqlalchemy.sql.elements import Label
from sqlalchemy.sql.expression import BinaryExpression
from sqlalchemy.sql.expression import ClauseList
from sqlalchemy.sql.expression import func
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.expression import tuple_
from sqlalchemy.sql.expression import UnaryExpression
from sqlalchemy.sql.expression import union
from sqlalchemy.sql.operators import ColumnOperators
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import combinations
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.testing import mock
from sqlalchemy.testing import ne_
from sqlalchemy.testing import resolve_lambda
from sqlalchemy.testing.assertions import expect_deprecated
from sqlalchemy.types import ARRAY
from sqlalchemy.types import Boolean
from sqlalchemy.types import Concatenable
from sqlalchemy.types import DateTime
from sqlalchemy.types import Indexable
from sqlalchemy.types import JSON
from sqlalchemy.types import MatchType
from sqlalchemy.types import NullType
from sqlalchemy.types import OperatorClass
from sqlalchemy.types import TypeDecorator
from sqlalchemy.types import TypeEngine
from sqlalchemy.types import UserDefinedType


class LoopOperate(operators.ColumnOperators):
    def operate(self, op, *other, **kwargs):
        return op


class ColExpressionDuckTypeOnly:
    def __init__(self, expr):
        self.expr = expr

    def __clause_element__(self):
        return self.expr


class DefaultColumnComparatorTest(
    testing.AssertsCompiledSQL, fixtures.TestBase
):
    dialect = __dialect__ = "default_enhanced"

    @testing.combinations((operators.desc_op, desc), (operators.asc_op, asc))
    def test_scalar(self, operator, compare_to):
        left = column("left")
        assert left.comparator.operate(operator).compare(compare_to(left))
        self._loop_test(operator)

    right_column = column("right")

    @testing.combinations(
        (operators.add, right_column),
        (operators.is_, None),
        (operators.is_not, None),
        (operators.isnot, None),  # deprecated 1.4; See #5429
        (operators.is_, null()),
        (operators.is_, true()),
        (operators.is_, false()),
        (operators.eq, True),
        (operators.ne, True),
        (operators.is_distinct_from, True),
        (operators.is_distinct_from, False),
        (operators.is_distinct_from, None),
        (operators.is_not_distinct_from, True),
        (operators.isnot_distinct_from, True),  # deprecated 1.4; See #5429
        (operators.is_, True),
        (operators.is_not, True),
        (operators.isnot, True),  # deprecated 1.4; See #5429
        (operators.is_, False),
        (operators.is_not, False),
        (operators.isnot, False),  # deprecated 1.4; See #5429
        (operators.like_op, right_column),
        (operators.not_like_op, right_column),
        (operators.notlike_op, right_column),  # deprecated 1.4; See #5435
        (operators.ilike_op, right_column),
        (operators.not_ilike_op, right_column),
        (operators.notilike_op, right_column),  # deprecated 1.4; See #5435
        (operators.is_, right_column),
        (operators.is_not, right_column),
        (operators.isnot, right_column),  # deprecated 1.4; See #5429
        (operators.concat_op, right_column),
        id_="ns",
    )
    def test_operate(self, operator, right):
        left = column("left")

        if operators.is_comparison(operator):
            type_ = sqltypes.BOOLEANTYPE
        else:
            type_ = sqltypes.NULLTYPE

        assert left.comparator.operate(operator, right).compare(
            BinaryExpression(
                coercions.expect(roles.WhereHavingRole, left),
                coercions.expect(roles.WhereHavingRole, right),
                operator,
                type_=type_,
            )
        )

        modifiers = operator(left, right).modifiers

        assert operator(left, right).compare(
            BinaryExpression(
                coercions.expect(roles.WhereHavingRole, left),
                coercions.expect(roles.WhereHavingRole, right),
                operator,
                modifiers=modifiers,
                type_=type_,
            )
        )

        self._loop_test(operator, right)

        if operators.is_comparison(operator):
            is_(
                left.comparator.operate(operator, right).type,
                sqltypes.BOOLEANTYPE,
            )

    def _loop_test(self, operator, *arg):
        loop = LoopOperate()
        is_(operator(loop, *arg), operator)

    @testing.combinations(
        operators.add,
        operators.and_,
        operators.or_,
        operators.mul,
        argnames="op",
    )
    def test_nonsensical_negations(self, op):
        opstring = compiler.OPERATORS[op]
        self.assert_compile(
            select(~op(column("x"), column("q"))),
            f"SELECT NOT (x{opstring}q) AS anon_1",
        )

    def test_null_true_false_is_sanity_checks(self):
        d = default.DefaultDialect()
        d.supports_native_boolean = True

        self.assert_compile(
            column("q") == None,
            "q IS NULL",
        )
        self.assert_compile(
            column("q") == null(),
            "q IS NULL",
        )
        # IS coercion only occurs from left to right (just discovered this)
        self.assert_compile(
            null() == column("q"),
            "NULL = q",
        )
        self.assert_compile(column("q") == true(), "q = true", dialect=d)
        self.assert_compile(true() == column("q"), "true = q", dialect=d)
        self.assert_compile(column("q") == True, "q = true", dialect=d)

        # this comes out reversed; no choice, column.__eq__() is called
        # and we don't get to know it's "reverse"
        self.assert_compile(True == column("q"), "q = true", dialect=d)

    def test_no_getitem(self):
        assert_raises_message(
            NotImplementedError,
            "Operator 'getitem' is not supported on this expression",
            self.test_operate,
            operators.getitem,
            column("right"),
        )
        assert_raises_message(
            NotImplementedError,
            "Operator 'getitem' is not supported on this expression",
            lambda: column("left")[3],
        )

    def test_in(self):
        left = column("left")
        assert left.comparator.operate(operators.in_op, [1, 2, 3]).compare(
            BinaryExpression(
                left,
                BindParameter(
                    "left", value=[1, 2, 3], unique=True, expanding=True
                ),
                operators.in_op,
                type_=sqltypes.BOOLEANTYPE,
            )
        )
        self._loop_test(operators.in_op, [1, 2, 3])

    def test_not_in(self):
        left = column("left")
        assert left.comparator.operate(operators.not_in_op, [1, 2, 3]).compare(
            BinaryExpression(
                left,
                BindParameter(
                    "left", value=[1, 2, 3], unique=True, expanding=True
                ),
                operators.not_in_op,
                type_=sqltypes.BOOLEANTYPE,
            )
        )
        self._loop_test(operators.not_in_op, [1, 2, 3])

    def test_in_no_accept_list_of_non_column_element(self):
        left = column("left")
        foo = ClauseList()
        assert_raises_message(
            exc.ArgumentError,
            r"IN expression list, SELECT construct, or bound parameter "
            r"object expected, got .*ClauseList",
            left.in_,
            [foo],
        )

    def test_in_no_accept_non_list_non_selectable(self):
        left = column("left")
        right = column("right")
        assert_raises_message(
            exc.ArgumentError,
            r"IN expression list, SELECT construct, or bound parameter "
            r"object expected, got .*ColumnClause",
            left.in_,
            right,
        )

    def test_in_no_accept_non_list_thing_with_getitem(self):
        # test [ticket:2726]
        class HasGetitem(String):
            class comparator_factory(String.Comparator):
                def __getitem__(self, value):
                    return value

        left = column("left")
        right = column("right", HasGetitem)
        assert_raises_message(
            exc.ArgumentError,
            r"IN expression list, SELECT construct, or bound parameter "
            r"object expected, got .*ColumnClause",
            left.in_,
            right,
        )

    def test_collate(self):
        left = column("left")
        right = "some collation"
        left.comparator.operate(operators.collate, right).compare(
            collate(left, right)
        )

    def test_collate_invalid_collation(self):
        left = column("left")
        right = "some.inva-lid.collation"
        with expect_raises_message(ValueError, "Invalid collation"):
            collate(left, right)
        with expect_raises_message(ValueError, "Invalid collation"):
            left.comparator.operate(operators.collate, right)

    def test_default_adapt(self):
        class TypeOne(TypeEngine):
            operator_classes = OperatorClass.ANY

        class TypeTwo(TypeEngine):
            operator_classes = OperatorClass.ANY

        expr = column("x", TypeOne()) - column("y", TypeTwo())
        is_(expr.type._type_affinity, TypeOne)

    def test_concatenable_adapt(self):
        class TypeOne(Concatenable, TypeEngine):
            operator_classes = OperatorClass.ANY

        class TypeTwo(Concatenable, TypeEngine):
            operator_classes = OperatorClass.ANY

        class TypeThree(TypeEngine):
            operator_classes = OperatorClass.ANY

        expr = column("x", TypeOne()) - column("y", TypeTwo())
        is_(expr.type._type_affinity, TypeOne)
        is_(expr.operator, operator.sub)

        expr = column("x", TypeOne()) + column("y", TypeTwo())
        is_(expr.type._type_affinity, TypeOne)
        is_(expr.operator, operators.concat_op)

        expr = column("x", TypeOne()) - column("y", TypeThree())
        is_(expr.type._type_affinity, TypeOne)
        is_(expr.operator, operator.sub)

        expr = column("x", TypeOne()) + column("y", TypeThree())
        is_(expr.type._type_affinity, TypeOne)
        is_(expr.operator, operator.add)

    def test_contains_override_raises(self):
        for col in [
            Column("x", String),
            Column("x", ARRAY(Integer)),
        ]:
            assert_raises_message(
                NotImplementedError,
                "Operator 'contains' is not supported on this expression",
                lambda: "foo" in col,
            )


class MultiElementExprTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.combinations(True, False, argnames="reverse")
    @testing.combinations(True, False, argnames="negate")
    def test_associatives_mismatched_type(self, reverse, negate):
        """test we get two separate exprs if the types dont match, operator
        is not lost.

        the expressions here don't generally make sense from a SQL
        perspective, we are checking just that the operators / parenthesis /
        negation works out in the SQL string to reasonably correspond to
        what the Python structures look like.

        """

        expr1 = column("i1", Integer) + column("i2", Integer)

        expr2 = column("d1", String) + column("d2", String)

        if reverse:
            expr = expr2 + expr1

            self.assert_compile(
                select(expr), "SELECT (d1 || d2) + i1 + i2 AS anon_1"
            )
        else:
            expr = expr1 + expr2

            self.assert_compile(
                select(expr), "SELECT i1 + i2 + (d1 || d2) AS anon_1"
            )

    @testing.combinations(
        operators.add,
        operators.mul,
        argnames="op",
    )
    @testing.combinations(True, False, argnames="reverse")
    @testing.combinations(True, False, argnames="negate")
    def test_parenthesized_exprs(self, op, reverse, negate):
        t1 = table("t", column("q"), column("p"))

        inner = lambda: t1.c.q - t1.c.p  # noqa E371
        expr = op(inner(), inner())

        if reverse:
            for i in range(8):
                expr = op(inner(), expr)
        else:
            for i in range(8):
                expr = op(expr, inner())

        opstring = compiler.OPERATORS[op]
        exprs = opstring.join("(t.q - t.p)" for i in range(10))

        if negate:
            self.assert_compile(
                select(~expr), f"SELECT NOT ({exprs}) AS anon_1 FROM t"
            )
        else:
            self.assert_compile(
                select(expr), f"SELECT {exprs} AS anon_1 FROM t"
            )

    @testing.combinations(
        (
            lambda p, q: (1 - p) * (2 - q) + 10 * (3 - p) * (4 - q),
            "(:p_1 - t.p) * (:q_1 - t.q) + "
            ":param_1 * (:p_2 - t.p) * (:q_2 - t.q)",
        ),
        (
            lambda p, q: (1 - p) * (2 - q) * (3 - p) * (4 - q),
            "(:p_1 - t.p) * (:q_1 - t.q) * (:p_2 - t.p) * (:q_2 - t.q)",
        ),
        (
            lambda p, q: (
                (1 + p + 5)
                * (p * (q - 5) * (p + 8))
                * (q + (p - 3) + (q - 5) + (p - 9))
                * (4 + q + 9)
            ),
            "(:p_1 + t.p + :param_1) * "
            "t.p * (t.q - :q_1) * (t.p + :p_2) * "
            "(t.q + (t.p - :p_3) + (t.q - :q_2) + (t.p - :p_4)) * "
            "(:q_3 + t.q + :param_2)",
        ),
        (
            lambda p, q: (1 // p) - (2 // q) - (3 // p) - (4 // q),
            "((:p_1 / t.p - :q_1 / t.q) - :p_2 / t.p) - :q_2 / t.q",
        ),
        (
            lambda p, q: (1 + p) - (2 + q) - (3 + p) - (4 + q),
            "(((:p_1 + t.p) - (:q_1 + t.q)) - (:p_2 + t.p)) - (:q_2 + t.q)",
        ),
        (
            lambda p, q: (1 + p) * 3 * (2 + q) * 4 * (3 + p) - (4 + q),
            "(:p_1 + t.p) * :param_1 * (:q_1 + t.q) * "
            ":param_2 * (:p_2 + t.p) - (:q_2 + t.q)",
        ),
        argnames="expr, expected",
    )
    def test_other_exprs(self, expr, expected):
        t = table("t", column("q", Integer), column("p", Integer))
        expr = resolve_lambda(expr, p=t.c.p, q=t.c.q)

        self.assert_compile(expr, expected)

    @testing.combinations(
        operators.add,
        operators.and_,
        operators.or_,
        operators.mul,
        argnames="op",
    )
    @testing.combinations(True, False, argnames="reverse")
    @testing.combinations(True, False, argnames="negate")
    def test_associatives(self, op, reverse, negate):
        t1 = table("t", column("q"), column("p"))

        num = 500

        expr = op(t1.c.q, t1.c.p)

        if reverse:
            for i in range(num - 1, -1, -1):
                expr = op(column(f"d{i}"), expr)
        else:
            for i in range(num):
                expr = op(expr, column(f"d{i}"))

        opstring = compiler.OPERATORS[op]
        exprs = opstring.join(f"d{i}" for i in range(num))

        if negate:
            self.assert_compile(
                select(~expr),
                (
                    f"SELECT NOT (t.q{opstring}t.p{opstring}{exprs}) "
                    "AS anon_1 FROM t"
                    if not reverse
                    else f"SELECT NOT ({exprs}{opstring}t.q{opstring}t.p) "
                    "AS anon_1 FROM t"
                ),
            )
        else:
            self.assert_compile(
                select(expr),
                (
                    f"SELECT t.q{opstring}t.p{opstring}{exprs} "
                    "AS anon_1 FROM t"
                    if not reverse
                    else f"SELECT {exprs}{opstring}t.q{opstring}t.p "
                    "AS anon_1 FROM t"
                ),
            )

    @testing.combinations(
        operators.gt,
        operators.eq,
        operators.le,
        operators.sub,
        argnames="op",
    )
    @testing.combinations(True, False, argnames="reverse")
    @testing.combinations(True, False, argnames="negate")
    def test_non_associatives(self, op, reverse, negate):
        """similar tests as test_associatives but for non-assoc
        operators.

        the expressions here don't generally make sense from a SQL
        perspective, we are checking just that the operators / parenthesis /
        negation works out in the SQL string to reasonably correspond to
        what the Python structures look like.

        """
        t1 = table("t", column("q"), column("p"))

        num = 5

        expr = op(t1.c.q, t1.c.p)

        if reverse:
            for i in range(num - 1, -1, -1):
                expr = op(column(f"d{i}"), expr)
        else:
            for i in range(num):
                expr = op(expr, column(f"d{i}"))

        opstring = compiler.OPERATORS[op]

        if negate:
            negate_op = {
                operators.gt: operators.le,
                operators.eq: operators.ne,
                operators.le: operators.gt,
            }.get(op, None)

            if negate_op:
                negate_opstring = compiler.OPERATORS[negate_op]
                if reverse:
                    str_expr = (
                        f"d0{negate_opstring}(d1{opstring}(d2{opstring}"
                        f"(d3{opstring}(d4{opstring}(t.q{opstring}t.p)))))"
                    )
                else:
                    str_expr = (
                        f"(((((t.q{opstring}t.p){opstring}d0){opstring}d1)"
                        f"{opstring}d2){opstring}d3){negate_opstring}d4"
                    )
            else:
                if reverse:
                    str_expr = (
                        f"NOT (d0{opstring}(d1{opstring}(d2{opstring}"
                        f"(d3{opstring}(d4{opstring}(t.q{opstring}t.p))))))"
                    )
                else:
                    str_expr = (
                        f"NOT ((((((t.q{opstring}t.p){opstring}d0)"
                        f"{opstring}d1){opstring}d2){opstring}d3){opstring}d4)"
                    )

            self.assert_compile(
                select(~expr),
                (
                    f"SELECT {str_expr} AS anon_1 FROM t"
                    if not reverse
                    else f"SELECT {str_expr} AS anon_1 FROM t"
                ),
            )
        else:
            if reverse:
                str_expr = (
                    f"d0{opstring}(d1{opstring}(d2{opstring}"
                    f"(d3{opstring}(d4{opstring}(t.q{opstring}t.p)))))"
                )
            else:
                str_expr = (
                    f"(((((t.q{opstring}t.p){opstring}d0)"
                    f"{opstring}d1){opstring}d2){opstring}d3){opstring}d4"
                )

            self.assert_compile(
                select(expr),
                (
                    f"SELECT {str_expr} AS anon_1 FROM t"
                    if not reverse
                    else f"SELECT {str_expr} AS anon_1 FROM t"
                ),
            )


class CustomUnaryOperatorTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def factorial(self):
        class MyInteger(Integer):
            class comparator_factory(Integer.Comparator):
                def factorial(self):
                    return UnaryExpression(
                        self.expr,
                        modifier=operators.custom_op("!"),
                        type_=MyInteger,
                    )

                def factorial_prefix(self):
                    return UnaryExpression(
                        self.expr,
                        operator=operators.custom_op("!!"),
                        type_=MyInteger,
                    )

                def __invert__(self):
                    return UnaryExpression(
                        self.expr,
                        operator=operators.custom_op("!!!"),
                        type_=MyInteger,
                    )

        return MyInteger

    @testing.fixture
    def modulus(self):
        class MyInteger(Integer):
            class comparator_factory(Integer.Comparator):
                def modulus(self):
                    return UnaryExpression(
                        self.expr,
                        modifier=operators.custom_op("%"),
                        type_=MyInteger,
                    )

                def modulus_prefix(self):
                    return UnaryExpression(
                        self.expr,
                        operator=operators.custom_op("%"),
                        type_=MyInteger,
                    )

        return MyInteger

    @testing.combinations(
        ("format",),
        ("qmark",),
        ("named",),
        ("pyformat",),
        argnames="paramstyle",
    )
    def test_modulus(self, modulus, paramstyle):
        col = column("somecol", modulus())
        self.assert_compile(
            col.modulus(),
            (
                "somecol %%"
                if paramstyle in ("format", "pyformat")
                else "somecol %"
            ),
            dialect=default.DefaultDialect(paramstyle=paramstyle),
        )

    @testing.combinations(
        ("format",),
        ("qmark",),
        ("named",),
        ("pyformat",),
        argnames="paramstyle",
    )
    def test_modulus_prefix(self, modulus, paramstyle):
        col = column("somecol", modulus())
        self.assert_compile(
            col.modulus_prefix(),
            (
                "%% somecol"
                if paramstyle in ("format", "pyformat")
                else "% somecol"
            ),
            dialect=default.DefaultDialect(paramstyle=paramstyle),
        )

    def test_factorial(self, factorial):
        col = column("somecol", factorial())
        self.assert_compile(col.factorial(), "somecol !")

    def test_double_factorial(self, factorial):
        col = column("somecol", factorial())
        self.assert_compile(col.factorial().factorial(), "somecol ! !")

    def test_factorial_prefix(self, factorial):
        col = column("somecol", factorial())
        self.assert_compile(col.factorial_prefix(), "!! somecol")

    def test_factorial_invert(self, factorial):
        col = column("somecol", factorial())
        self.assert_compile(~col, "!!! somecol")

    def test_double_factorial_invert(self, factorial):
        col = column("somecol", factorial())
        self.assert_compile(~(~col), "!!! (!!! somecol)")

    def test_unary_no_ops(self):
        assert_raises_message(
            exc.CompileError,
            "Unary expression has no operator or modifier",
            UnaryExpression(literal("x")).compile,
        )

    def test_unary_both_ops(self):
        assert_raises_message(
            exc.CompileError,
            "Unary expression does not support operator and "
            "modifier simultaneously",
            UnaryExpression(
                literal("x"),
                operator=operators.custom_op("x"),
                modifier=operators.custom_op("y"),
            ).compile,
        )


class _CustomComparatorTests:
    def test_override_builtin(self):
        c1 = Column("foo", self._add_override_factory())
        self._assert_add_override(c1)

    def test_subquery_proxy(self):
        t = Table("t", MetaData(), Column("foo", self._add_override_factory()))
        proxied = t.select().subquery().c.foo
        self._assert_add_override(proxied)
        self._assert_and_override(proxied)

    def test_alias_proxy(self):
        t = Table("t", MetaData(), Column("foo", self._add_override_factory()))
        proxied = t.alias().c.foo
        self._assert_add_override(proxied)
        self._assert_and_override(proxied)

    def test_binary_propagate(self):
        c1 = Column("foo", self._add_override_factory())
        self._assert_add_override(c1 - 6)
        self._assert_and_override(c1 - 6)

    def test_reverse_binary_propagate(self):
        c1 = Column("foo", self._add_override_factory())
        self._assert_add_override(6 - c1)
        self._assert_and_override(6 - c1)

    def test_binary_multi_propagate(self):
        c1 = Column("foo", self._add_override_factory())
        self._assert_add_override((c1 - 6) + 5)
        self._assert_and_override((c1 - 6) + 5)

    def test_no_boolean_propagate(self):
        c1 = Column("foo", self._add_override_factory())

        class Nonsensical(Boolean):
            operator_classes = OperatorClass.BOOLEAN | OperatorClass.NUMERIC

        expr = c1 == 56
        expr.type = Nonsensical()
        self._assert_not_add_override(expr)

        self._assert_not_and_override(c1 == 56)

    def _assert_and_override(self, expr):
        assert (expr & text("5")).compare(expr.op("goofy_and")(text("5")))

    def _assert_add_override(self, expr):
        assert (expr + 5).compare(expr.op("goofy")(5))

    def _assert_not_add_override(self, expr):
        assert not (expr + 5).compare(expr.op("goofy")(5))

    def _assert_not_and_override(self, expr):
        assert not (expr & text("5")).compare(expr.op("goofy_and")(text("5")))


class CustomComparatorTest(_CustomComparatorTests, fixtures.TestBase):
    def _add_override_factory(self):
        class MyInteger(Integer):
            class comparator_factory(TypeEngine.Comparator):
                def __init__(self, expr):
                    super().__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        return MyInteger


class TypeDecoratorComparatorTest(_CustomComparatorTests, fixtures.TestBase):
    def _add_override_factory(self):
        class MyInteger(TypeDecorator):
            impl = Integer
            cache_ok = True

            class comparator_factory(TypeDecorator.Comparator):
                def __init__(self, expr):
                    super().__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        return MyInteger


class TypeDecoratorTypeDecoratorComparatorTest(
    _CustomComparatorTests, fixtures.TestBase
):
    def _add_override_factory(self):
        class MyIntegerOne(TypeDecorator):
            impl = Integer
            cache_ok = True

            class comparator_factory(TypeDecorator.Comparator):
                def __init__(self, expr):
                    super().__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        class MyIntegerTwo(TypeDecorator):
            impl = MyIntegerOne
            cache_ok = True

        return MyIntegerTwo


class TypeDecoratorWVariantComparatorTest(
    _CustomComparatorTests, fixtures.TestBase
):
    def _add_override_factory(self):
        class SomeOtherInteger(Integer):
            class comparator_factory(TypeEngine.Comparator):
                def __init__(self, expr):
                    super().__init__(expr)

                def __add__(self, other):
                    return self.expr.op("not goofy")(other)

                def __and__(self, other):
                    return self.expr.op("not goofy_and")(other)

        class MyInteger(TypeDecorator):
            impl = Integer
            cache_ok = True

            class comparator_factory(TypeDecorator.Comparator):
                def __init__(self, expr):
                    super().__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        return MyInteger().with_variant(SomeOtherInteger, "mysql")


class CustomEmbeddedinTypeDecoratorTest(
    _CustomComparatorTests, fixtures.TestBase
):
    def _add_override_factory(self):
        class MyInteger(Integer):
            class comparator_factory(TypeEngine.Comparator):
                def __init__(self, expr):
                    super().__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        class MyDecInteger(TypeDecorator):
            impl = MyInteger
            cache_ok = True

        return MyDecInteger


class NewOperatorTest(_CustomComparatorTests, fixtures.TestBase):
    def _add_override_factory(self):
        class MyInteger(Integer):
            class comparator_factory(TypeEngine.Comparator):
                def __init__(self, expr):
                    super().__init__(expr)

                def foob(self, other):
                    return self.expr.op("foob")(other)

        return MyInteger

    def _assert_add_override(self, expr):
        assert (expr.foob(5)).compare(expr.op("foob")(5))

    def _assert_not_add_override(self, expr):
        assert not hasattr(expr, "foob")

    def _assert_and_override(self, expr):
        pass

    def _assert_not_and_override(self, expr):
        pass


class OperatorClassTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    """test operator classes introduced in #12736"""

    __dialect__ = "default"

    def test_no_class(self):
        class MyType(TypeEngine):
            pass

        with expect_deprecated(
            r"Type object .*.MyType.* does not refer to an OperatorClass"
        ):
            column("q", MyType()) + 5

    @testing.variation("json_type", ["plain", "with_variant"])
    def test_json_cant_contains(self, json_type):
        """test the original case for #12736"""

        if json_type.plain:
            type_ = JSON()
        else:
            type_ = JSON().with_variant(JSONB(), "postgresql")

        with expect_deprecated(
            r"Type object .*.JSON.* does not include operator "
            r"'contains_op' in its operator classes."
        ):
            self.assert_compile(
                column("xyz", type_).contains("{'foo': 'bar'}"),
                "xyz LIKE '%' || :xyz_1 || '%'",
            )

    def test_invalid_op(self):
        with expect_deprecated(
            r"Type object .*.Integer.* does not include "
            "operator 'like_op' in its operator classes."
        ):
            expr = column("q", Integer).like("hi")

            self.assert_compile(expr, "q LIKE :q_1", checkparams={"q_1": "hi"})

    def test_invalid_op_custom(self):
        class MyType(Integer):
            pass

        with expect_deprecated(
            r"Type object .*.MyType.* does not include "
            "operator 'like_op' in its operator classes."
        ):
            expr = column("q", MyType).like("hi")

            self.assert_compile(expr, "q LIKE :q_1", checkparams={"q_1": "hi"})

    def test_add_in_classes(self):
        class MyType(Integer):
            operator_classes = (
                Integer.operator_classes | OperatorClass.STRING_MATCH
            )

        expr = column("q", MyType).like("hi")

        self.assert_compile(expr, "q LIKE :q_1", checkparams={"q_1": "hi"})


class ExtensionOperatorTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    def test_contains(self):
        class MyType(UserDefinedType):
            cache_ok = True

            class comparator_factory(UserDefinedType.Comparator):
                def contains(self, other, **kw):
                    return self.op("->")(other)

        self.assert_compile(Column("x", MyType()).contains(5), "x -> :x_1")

    def test_getitem(self):
        class MyType(UserDefinedType):
            cache_ok = True

            class comparator_factory(UserDefinedType.Comparator):
                def __getitem__(self, index):
                    return self.op("->")(index)

        self.assert_compile(Column("x", MyType())[5], "x -> :x_1")

    def test_op_not_an_iterator(self):
        # see [ticket:2726]
        class MyType(UserDefinedType):
            class comparator_factory(UserDefinedType.Comparator):
                def __getitem__(self, index):
                    return self.op("->")(index)

        col = Column("x", MyType())
        assert not isinstance(col, collections_abc.Iterable)

    @testing.combinations(
        (operators.lshift, OperatorClass.BITWISE),
        (operators.rshift, OperatorClass.BITWISE),
        (operators.matmul, OperatorClass.MATH),
        (operators.getitem, OperatorClass.INDEXABLE),
    )
    def test_not_implemented_operators(self, op, operator_class):
        """test operators that are availble but not implemented by default.

        this might be semantically different from the operator not being
        present in the operator class though the effect is the same (that is,
        we could just not include lshift/rshift/matmul in any operator class,
        do away with _unsupported_impl() and the path to implement them would
        be the same).   So it's not totally clear if we should keep using
        _unsupported_impl() long term.  However at least for now because we
        only emit a deprecation warning in the other case, this is still
        appropriately a separate concept.

        """

        class MyType(TypeEngine):
            operator_classes = operator_class

        with expect_raises_message(
            NotImplementedError,
            f"Operator {op.__name__!r} is not supported on this expression",
        ):
            op(column("q", MyType()), "test")

    def test_lshift(self):
        class MyType(UserDefinedType):
            cache_ok = True

            class comparator_factory(UserDefinedType.Comparator):
                def __lshift__(self, other):
                    return self.op("->")(other)

        self.assert_compile(Column("x", MyType()) << 5, "x -> :x_1")

    def test_rlshift(self):
        class MyType(UserDefinedType):
            cache_ok = True

            class comparator_factory(UserDefinedType.Comparator):
                def __rlshift__(self, other):
                    return self.op("->")(other)

        self.assert_compile(5 << Column("x", MyType()), "x -> :x_1")

    def test_rshift(self):
        class MyType(UserDefinedType):
            cache_ok = True

            class comparator_factory(UserDefinedType.Comparator):
                def __rshift__(self, other):
                    return self.op("->")(other)

        self.assert_compile(Column("x", MyType()) >> 5, "x -> :x_1")

    def test_rrshift(self):
        class MyType(UserDefinedType):
            cache_ok = True

            class comparator_factory(UserDefinedType.Comparator):
                def __rrshift__(self, other):
                    return self.op("->")(other)

        self.assert_compile(5 >> Column("x", MyType()), "x -> :x_1")

    def test_matmul(self):
        class MyType(UserDefinedType):
            cache_ok = True

            class comparator_factory(UserDefinedType.Comparator):
                def __matmul__(self, other):
                    return self.op("->")(other)

        self.assert_compile(Column("x", MyType()) @ 5, "x -> :x_1")

    def test_rmatmul(self):
        class MyType(UserDefinedType):
            cache_ok = True

            class comparator_factory(UserDefinedType.Comparator):
                def __rmatmul__(self, other):
                    return self.op("->")(other)

        self.assert_compile(5 @ Column("x", MyType()), "x -> :x_1")


class JSONIndexOpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    def setup_test(self):
        class MyTypeCompiler(compiler.GenericTypeCompiler):
            def visit_mytype(self, type_, **kw):
                return "MYTYPE"

            def visit_myothertype(self, type_, **kw):
                return "MYOTHERTYPE"

        class MyCompiler(compiler.SQLCompiler):
            def visit_json_getitem_op_binary(self, binary, operator, **kw):
                return self._generate_generic_binary(
                    binary, " -> ", eager_grouping=True, **kw
                )

            def visit_json_path_getitem_op_binary(
                self, binary, operator, **kw
            ):
                return self._generate_generic_binary(
                    binary, " #> ", eager_grouping=True, **kw
                )

            def visit_getitem_binary(self, binary, operator, **kw):
                raise NotImplementedError()

        class MyDialect(default.DefaultDialect):
            statement_compiler = MyCompiler
            type_compiler = MyTypeCompiler

        class MyType(JSON):
            __visit_name__ = "mytype"

            operator_classes = OperatorClass.JSON | OperatorClass.MATH

        self.MyType = MyType
        self.__dialect__ = MyDialect()

    def test_setup_getitem(self):
        col = Column("x", self.MyType())

        is_(col[5].type._type_affinity, JSON)
        is_(col[5]["foo"].type._type_affinity, JSON)
        is_(col[("a", "b", "c")].type._type_affinity, JSON)

    @testing.combinations(
        (lambda col: col["foo"] + " ", "(x -> :x_1) || :param_1"),
        (
            lambda col: col["foo"] + " " + col["bar"],
            "(x -> :x_1) || :param_1 || (x -> :x_2)",
        ),
    )
    def test_eager_grouping_flag(self, expr, expected):
        """test #10479"""
        col = Column("x", self.MyType())

        expr = testing.resolve_lambda(expr, col=col)

        self.assert_compile(expr, expected)

    def test_getindex_literal_integer(self):
        col = Column("x", self.MyType())

        self.assert_compile(col[5], "x -> :x_1", checkparams={"x_1": 5})

    def test_getindex_literal_string(self):
        col = Column("x", self.MyType())

        self.assert_compile(
            col["foo"], "x -> :x_1", checkparams={"x_1": "foo"}
        )

    def test_path_getindex_literal(self):
        col = Column("x", self.MyType())

        self.assert_compile(
            col[("a", "b", 3, 4, "d")],
            "x #> :x_1",
            checkparams={"x_1": ("a", "b", 3, 4, "d")},
        )

    def test_getindex_sqlexpr(self):
        col = Column("x", self.MyType())
        col2 = Column("y", Integer())

        self.assert_compile(col[col2], "x -> y", checkparams={})

    def test_getindex_sqlexpr_right_grouping(self):
        col = Column("x", self.MyType())
        col2 = Column("y", Integer())

        self.assert_compile(
            col[col2 + 8], "x -> (y + :y_1)", checkparams={"y_1": 8}
        )

    def test_getindex_sqlexpr_left_grouping(self):
        col = Column("x", self.MyType())

        self.assert_compile(col[8] != None, "(x -> :x_1) IS NOT NULL")  # noqa

    def test_getindex_sqlexpr_both_grouping(self):
        col = Column("x", self.MyType())
        col2 = Column("y", Integer())

        self.assert_compile(
            col[col2 + 8] != None,  # noqa
            "(x -> (y + :y_1)) IS NOT NULL",
            checkparams={"y_1": 8},
        )

    def test_override_operators(self):
        special_index_op = operators.custom_op("$$>")

        class MyOtherType(JSON, TypeEngine):
            __visit_name__ = "myothertype"

            class Comparator(TypeEngine.Comparator):
                def _adapt_expression(self, op, other_comparator):
                    return special_index_op, MyOtherType()

            comparator_factory = Comparator

        col = Column("x", MyOtherType())
        self.assert_compile(col[5], "x $$> :x_1", checkparams={"x_1": 5})

    def _caster_combinations(fn):
        return testing.combinations(
            ("integer", Integer),
            ("boolean", Boolean),
            ("float", Float),
            ("string", String),
        )(fn)

    @_caster_combinations
    def test_cast_ops(self, caster, expected_type):
        expr = Column("x", JSON)["foo"]

        expr = getattr(expr, "as_%s" % caster)()
        is_(expr.type._type_affinity, expected_type)

    @_caster_combinations
    def test_cast_ops_unsupported_on_non_binary(self, caster, expected_type):
        expr = Column("x", JSON)

        meth = getattr(expr, "as_%s" % caster)

        assert_raises_message(
            exc.InvalidRequestError,
            r"The JSON cast operator JSON.as_%s\(\) only works" % caster,
            meth,
        )

    @_caster_combinations
    def test_cast_ops_unsupported_on_non_json_binary(
        self, caster, expected_type
    ):
        expr = Column("x", self.MyType) + {"foo": "bar"}

        meth = getattr(expr, "as_%s" % caster)

        assert_raises_message(
            exc.InvalidRequestError,
            r"The JSON cast operator JSON.as_%s\(\) only works" % caster,
            meth,
        )


class ArrayIndexOpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    def setup_test(self):
        class MyTypeCompiler(compiler.GenericTypeCompiler):
            def visit_mytype(self, type_, **kw):
                return "MYTYPE"

            def visit_myothertype(self, type_, **kw):
                return "MYOTHERTYPE"

        class MyCompiler(compiler.SQLCompiler):
            def visit_slice(self, element, **kw):
                return "%s:%s" % (
                    self.process(element.start, **kw),
                    self.process(element.stop, **kw),
                )

            def visit_getitem_binary(self, binary, operator, **kw):
                return "%s[%s]" % (
                    self.process(binary.left, **kw),
                    self.process(binary.right, **kw),
                )

        class MyDialect(default.DefaultDialect):
            statement_compiler = MyCompiler
            type_compiler = MyTypeCompiler

        class MyType(ARRAY):
            __visit_name__ = "mytype"

            def __init__(self, zero_indexes=False, dimensions=1):
                if zero_indexes:
                    self.zero_indexes = zero_indexes
                self.dimensions = dimensions
                self.item_type = Integer()

        self.MyType = MyType
        self.__dialect__ = MyDialect()

    def test_setup_getitem_w_dims(self):
        """test the behavior of the _setup_getitem() method given a simple
        'dimensions' scheme - this is identical to postgresql.ARRAY."""

        col = Column("x", self.MyType(dimensions=3))

        is_(col[5].type._type_affinity, ARRAY)
        eq_(col[5].type.dimensions, 2)
        is_(col[5][6].type._type_affinity, ARRAY)
        eq_(col[5][6].type.dimensions, 1)
        is_(col[5][6][7].type._type_affinity, Integer)

    def test_getindex_literal(self):
        col = Column("x", self.MyType())

        self.assert_compile(col[5], "x[:x_1]", checkparams={"x_1": 5})

    def test_contains_override_raises(self):
        col = Column("x", self.MyType())

        assert_raises_message(
            NotImplementedError,
            "Operator 'contains' is not supported on this expression",
            lambda: "foo" in col,
        )

    def test_getindex_sqlexpr(self):
        col = Column("x", self.MyType())
        col2 = Column("y", Integer())

        self.assert_compile(col[col2], "x[y]", checkparams={})

        self.assert_compile(
            col[col2 + 8], "x[(y + :y_1)]", checkparams={"y_1": 8}
        )

    def test_getslice_literal(self):
        col = Column("x", self.MyType())

        self.assert_compile(
            col[5:6], "x[:x_1::x_2]", checkparams={"x_1": 5, "x_2": 6}
        )

    def test_getslice_sqlexpr(self):
        col = Column("x", self.MyType())
        col2 = Column("y", Integer())

        self.assert_compile(
            col[col2 : col2 + 5], "x[y:y + :y_1]", checkparams={"y_1": 5}
        )

    def test_getindex_literal_zeroind(self):
        col = Column("x", self.MyType(zero_indexes=True))

        self.assert_compile(col[5], "x[:x_1]", checkparams={"x_1": 6})

    def test_getindex_sqlexpr_zeroind(self):
        col = Column("x", self.MyType(zero_indexes=True))
        col2 = Column("y", Integer())

        self.assert_compile(col[col2], "x[(y + :y_1)]", checkparams={"y_1": 1})

        self.assert_compile(
            col[col2 + 8],
            "x[(y + :y_1 + :param_1)]",
            checkparams={"y_1": 8, "param_1": 1},
        )

    def test_getslice_literal_zeroind(self):
        col = Column("x", self.MyType(zero_indexes=True))

        self.assert_compile(
            col[5:6], "x[:x_1::x_2]", checkparams={"x_1": 6, "x_2": 7}
        )

    def test_getslice_sqlexpr_zeroind(self):
        col = Column("x", self.MyType(zero_indexes=True))
        col2 = Column("y", Integer())

        self.assert_compile(
            col[col2 : col2 + 5],
            "x[y + :y_1:y + :y_2 + :param_1]",
            checkparams={"y_1": 1, "y_2": 5, "param_1": 1},
        )

    def test_override_operators(self):
        special_index_op = operators.custom_op("->")

        class MyOtherType(Indexable, TypeEngine):
            __visit_name__ = "myothertype"

            operator_classes = OperatorClass.ANY

            class Comparator(TypeEngine.Comparator):
                def _adapt_expression(self, op, other_comparator):
                    return special_index_op, MyOtherType()

            comparator_factory = Comparator

        col = Column("x", MyOtherType())
        self.assert_compile(col[5], "x -> :x_1", checkparams={"x_1": 5})


class BooleanEvalTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    """test standalone booleans being wrapped in an AsBoolean, as well
    as true/false compilation."""

    def _dialect(self, native_boolean):
        d = default.DefaultDialect()
        d.supports_native_boolean = native_boolean
        return d

    def test_one(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).where(c),
            "SELECT x WHERE x",
            dialect=self._dialect(True),
        )

    def test_two_a(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).where(c),
            "SELECT x WHERE x = 1",
            dialect=self._dialect(False),
        )

    def test_two_b(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).where(c),
            "SELECT x WHERE x = 1",
            dialect=self._dialect(False),
        )

    def test_three_a(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).where(~c),
            "SELECT x WHERE x = 0",
            dialect=self._dialect(False),
        )

    def test_three_a_double(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).where(~~c),
            "SELECT x WHERE x = 1",
            dialect=self._dialect(False),
        )

    def test_three_b(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).where(~c),
            "SELECT x WHERE x = 0",
            dialect=self._dialect(False),
        )

    def test_four(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).where(~c),
            "SELECT x WHERE NOT x",
            dialect=self._dialect(True),
        )

    def test_four_double(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).where(~~c),
            "SELECT x WHERE x",
            dialect=self._dialect(True),
        )

    def test_five_a(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).having(c),
            "SELECT x HAVING x = 1",
            dialect=self._dialect(False),
        )

    def test_five_b(self):
        c = column("x", Boolean)
        self.assert_compile(
            select(c).having(c),
            "SELECT x HAVING x = 1",
            dialect=self._dialect(False),
        )

    def test_six(self):
        self.assert_compile(
            or_(false(), true()), "1 = 1", dialect=self._dialect(False)
        )

    def test_seven_a(self):
        t1 = table("t1", column("a"))
        t2 = table("t2", column("b"))
        self.assert_compile(
            join(t1, t2, onclause=true()),
            "t1 JOIN t2 ON 1 = 1",
            dialect=self._dialect(False),
        )

    def test_seven_b(self):
        t1 = table("t1", column("a"))
        t2 = table("t2", column("b"))
        self.assert_compile(
            join(t1, t2, onclause=false()),
            "t1 JOIN t2 ON 0 = 1",
            dialect=self._dialect(False),
        )

    def test_seven_c(self):
        t1 = table("t1", column("a"))
        t2 = table("t2", column("b"))
        self.assert_compile(
            join(t1, t2, onclause=true()),
            "t1 JOIN t2 ON true",
            dialect=self._dialect(True),
        )

    def test_seven_d(self):
        t1 = table("t1", column("a"))
        t2 = table("t2", column("b"))
        self.assert_compile(
            join(t1, t2, onclause=false()),
            "t1 JOIN t2 ON false",
            dialect=self._dialect(True),
        )

    def test_eight(self):
        self.assert_compile(
            and_(false(), true()), "false", dialect=self._dialect(True)
        )

    def test_nine(self):
        self.assert_compile(
            and_(false(), true()), "0 = 1", dialect=self._dialect(False)
        )

    def test_ten(self):
        c = column("x", Boolean)
        self.assert_compile(c == 1, "x = :x_1", dialect=self._dialect(False))

    def test_eleven(self):
        c = column("x", Boolean)
        self.assert_compile(
            c.is_(true()), "x IS true", dialect=self._dialect(True)
        )

    def test_twelve(self):
        c = column("x", Boolean)
        # I don't have a solution for this one yet,
        # other than adding some heavy-handed conditionals
        # into compiler
        self.assert_compile(
            c.is_(true()), "x IS 1", dialect=self._dialect(False)
        )


class ConjunctionTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    """test interaction of and_()/or_() with boolean , null constants"""

    __dialect__ = default.DefaultDialect(supports_native_boolean=True)

    def test_single_bool_one(self):
        self.assert_compile(~and_(true()), "false")

    def test_single_bool_two(self):
        self.assert_compile(~and_(True), "false")

    def test_single_bool_three(self):
        self.assert_compile(or_(~and_(true())), "false")

    def test_single_bool_four(self):
        self.assert_compile(~or_(false()), "true")

    def test_single_bool_five(self):
        self.assert_compile(~or_(False), "true")

    def test_single_bool_six(self):
        self.assert_compile(and_(~or_(false())), "true")

    def test_single_bool_seven(self):
        self.assert_compile(and_(True), "true")

    def test_single_bool_eight(self):
        self.assert_compile(or_(False), "false")

    def test_single_bool_nine(self):
        self.assert_compile(
            and_(True),
            "1 = 1",
            dialect=default.DefaultDialect(supports_native_boolean=False),
        )

    def test_single_bool_ten(self):
        self.assert_compile(
            or_(False),
            "0 = 1",
            dialect=default.DefaultDialect(supports_native_boolean=False),
        )

    @combinations(
        (and_, "and_", r"true", "True"),
        (or_, "or_", r"false", "False"),
    )
    def test_empty_clauses(self, op, str_op, str_continue, str_continue_2):
        # these warning classes will change to ArgumentError when the
        # deprecated behavior is disabled

        with expect_deprecated(
            re.escape(
                f"Invoking {str_op}() without arguments is deprecated, and "
                "will be disallowed in a future release.   For an empty "
                f"{str_op}() construct, use "
                f"'{str_op}({str_continue}(), *args)' or "
                f"'{str_op}({str_continue_2}, *args)'."
            )
        ):
            op()

    def test_empty_construct_for_whereclause(self):
        eq_(BooleanClauseList._construct_for_whereclause(()), None)

    def test_non_empty_construct_for_whereclause(self):
        self.assert_compile(
            BooleanClauseList._construct_for_whereclause([column("q") == 5]),
            "q = :q_1",
        )

    def test_empty_and_raw(self):
        self.assert_compile(
            BooleanClauseList._construct_raw(operators.and_), ""
        )

    def test_empty_or_raw(self):
        self.assert_compile(
            BooleanClauseList._construct_raw(operators.and_), ""
        )

    def test_four(self):
        x = column("x")
        self.assert_compile(
            and_(or_(x == 5), or_(x == 7)), "x = :x_1 AND x = :x_2"
        )

    def test_five(self):
        x = column("x")
        self.assert_compile(and_(true()._ifnone(None), x == 7), "x = :x_1")

    def test_six(self):
        x = column("x")
        self.assert_compile(or_(true(), x == 7), "true")
        self.assert_compile(or_(x == 7, true()), "true")
        self.assert_compile(~or_(x == 7, true()), "false")

    def test_six_pt_five(self):
        x = column("x")
        self.assert_compile(
            select(x).where(or_(x == 7, true())), "SELECT x WHERE true"
        )

        self.assert_compile(
            select(x).where(or_(x == 7, true())),
            "SELECT x WHERE 1 = 1",
            dialect=default.DefaultDialect(supports_native_boolean=False),
        )

    def test_seven(self):
        x = column("x")
        self.assert_compile(
            and_(true(), x == 7, true(), x == 9), "x = :x_1 AND x = :x_2"
        )

    def test_eight(self):
        x = column("x")
        self.assert_compile(
            or_(false(), x == 7, false(), x == 9), "x = :x_1 OR x = :x_2"
        )

    def test_nine(self):
        x = column("x")
        self.assert_compile(and_(x == 7, x == 9, false(), x == 5), "false")
        self.assert_compile(~and_(x == 7, x == 9, false(), x == 5), "true")

    def test_ten(self):
        self.assert_compile(and_(None, None), "NULL AND NULL")

    def test_eleven(self):
        x = column("x")
        self.assert_compile(
            select(x).where(None).where(None), "SELECT x WHERE NULL AND NULL"
        )

    def test_twelve(self):
        x = column("x")
        self.assert_compile(
            select(x).where(and_(None, None)), "SELECT x WHERE NULL AND NULL"
        )

    def test_thirteen(self):
        x = column("x")
        self.assert_compile(
            select(x).where(~and_(None, None)),
            "SELECT x WHERE NOT (NULL AND NULL)",
        )

    def test_fourteen(self):
        x = column("x")
        self.assert_compile(
            select(x).where(~null()), "SELECT x WHERE NOT NULL"
        )

    def test_constants_are_singleton(self):
        is_(null(), null())
        is_(false(), false())
        is_(true(), true())

    def test_constant_render_distinct(self):
        self.assert_compile(
            select(null(), null()), "SELECT NULL AS anon_1, NULL AS anon__1"
        )
        self.assert_compile(
            select(true(), true()), "SELECT true AS anon_1, true AS anon__1"
        )
        self.assert_compile(
            select(false(), false()),
            "SELECT false AS anon_1, false AS anon__1",
        )

    def test_constant_render_distinct_use_labels(self):
        self.assert_compile(
            select(null(), null()).set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ),
            "SELECT NULL AS anon_1, NULL AS anon__1",
        )
        self.assert_compile(
            select(true(), true()).set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ),
            "SELECT true AS anon_1, true AS anon__1",
        )
        self.assert_compile(
            select(false(), false()).set_label_style(
                LABEL_STYLE_TABLENAME_PLUS_COL
            ),
            "SELECT false AS anon_1, false AS anon__1",
        )

    def test_is_true_literal(self):
        c = column("x", Boolean)
        self.assert_compile(c.is_(True), "x IS true")

    def test_is_false_literal(self):
        c = column("x", Boolean)
        self.assert_compile(c.is_(False), "x IS false")

    def test_and_false_literal_leading(self):
        self.assert_compile(and_(False, True), "false")

        self.assert_compile(and_(False, False), "false")

    def test_and_true_literal_leading(self):
        self.assert_compile(and_(True, True), "true")

        self.assert_compile(and_(True, False), "false")

    def test_or_false_literal_leading(self):
        self.assert_compile(or_(False, True), "true")

        self.assert_compile(or_(False, False), "false")

    def test_or_true_literal_leading(self):
        self.assert_compile(or_(True, True), "true")

        self.assert_compile(or_(True, False), "true")


class OperatorPrecedenceTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table(
        "mytable",
        column("myid", Integer),
        column("name", String),
        column("description", String),
    )

    table2 = table("op", column("field"))

    def test_operator_precedence_1(self):
        self.assert_compile(
            self.table2.select().where(
                (self.table2.c.field == 5) == None
            ),  # noqa
            "SELECT op.field FROM op WHERE (op.field = :field_1) IS NULL",
        )

    def test_operator_precedence_2(self):
        self.assert_compile(
            self.table2.select().where(
                (self.table2.c.field + 5) == self.table2.c.field
            ),
            "SELECT op.field FROM op WHERE op.field + :field_1 = op.field",
        )

    def test_operator_precedence_3(self):
        self.assert_compile(
            self.table2.select().where((self.table2.c.field + 5) * 6),
            "SELECT op.field FROM op WHERE (op.field + :field_1) * :param_1",
        )

    def test_operator_precedence_4(self):
        self.assert_compile(
            self.table2.select().where((self.table2.c.field * 5) + 6),
            "SELECT op.field FROM op WHERE op.field * :field_1 + :param_1",
        )

    def test_operator_precedence_5(self):
        self.assert_compile(
            self.table2.select().where(
                5 + type_coerce(self.table2.c.field.in_([5, 6]), Integer)
            ),
            "SELECT op.field FROM op WHERE :param_1 + "
            "(op.field IN (__[POSTCOMPILE_field_1]))",
        )

    def test_operator_precedence_6(self):
        self.assert_compile(
            self.table2.select().where((5 + self.table2.c.field).in_([5, 6])),
            "SELECT op.field FROM op WHERE :field_1 + op.field "
            "IN (__[POSTCOMPILE_param_1])",
        )

    def test_operator_precedence_7(self):
        self.assert_compile(
            self.table2.select().where(
                not_(and_(self.table2.c.field == 5, self.table2.c.field == 7))
            ),
            "SELECT op.field FROM op WHERE NOT "
            "(op.field = :field_1 AND op.field = :field_2)",
        )

    def test_operator_precedence_8(self):
        self.assert_compile(
            self.table2.select().where(not_(self.table2.c.field == 5)),
            "SELECT op.field FROM op WHERE op.field != :field_1",
        )

    def test_operator_precedence_9(self):
        self.assert_compile(
            self.table2.select().where(
                not_(self.table2.c.field.between(5, 6))
            ),
            "SELECT op.field FROM op WHERE "
            "op.field NOT BETWEEN :field_1 AND :field_2",
        )

    def test_operator_precedence_10(self):
        self.assert_compile(
            self.table2.select().where(not_(self.table2.c.field) == 5),
            "SELECT op.field FROM op WHERE (NOT op.field) = :param_1",
        )

    def test_operator_precedence_11(self):
        self.assert_compile(
            self.table2.select().where(
                (self.table2.c.field == self.table2.c.field).between(
                    False, True
                )
            ),
            "SELECT op.field FROM op WHERE (op.field = op.field) "
            "BETWEEN :param_1 AND :param_2",
        )

    def test_operator_precedence_12(self):
        self.assert_compile(
            self.table2.select().where(
                between(
                    (self.table2.c.field == self.table2.c.field), False, True
                )
            ),
            "SELECT op.field FROM op WHERE (op.field = op.field) "
            "BETWEEN :param_1 AND :param_2",
        )

    def test_operator_precedence_13(self):
        self.assert_compile(
            self.table2.select().where(
                self.table2.c.field.match(self.table2.c.field).is_(None)
            ),
            "SELECT op.field FROM op WHERE (op.field MATCH op.field) IS NULL",
        )

    def test_operator_precedence_collate_1(self):
        self.assert_compile(
            self.table1.c.name == literal("foo").collate("utf-8"),
            'mytable.name = (:param_1 COLLATE "utf-8")',
        )

    def test_operator_precedence_collate_2(self):
        self.assert_compile(
            type_coerce(self.table1.c.name == literal("foo"), String).collate(
                "utf-8"
            ),
            'mytable.name = :param_1 COLLATE "utf-8"',
        )

    def test_operator_precedence_collate_3(self):
        self.assert_compile(
            self.table1.c.name.collate("utf-8") == "foo",
            '(mytable.name COLLATE "utf-8") = :param_1',
        )

    def test_operator_precedence_collate_4(self):
        class Nonsensical(Boolean):
            operator_classes = OperatorClass.BOOLEAN | OperatorClass.STRING

        self.assert_compile(
            and_(
                type_coerce(
                    self.table1.c.name == literal("foo"), Nonsensical
                ).collate("utf-8"),
                type_coerce(
                    self.table2.c.field == literal("bar"), Nonsensical
                ).collate("utf-8"),
            ),
            'mytable.name = :param_1 COLLATE "utf-8" '
            'AND op.field = :param_2 COLLATE "utf-8"',
        )

    def test_operator_precedence_collate_5(self):
        self.assert_compile(
            select(self.table1.c.name).order_by(
                self.table1.c.name.collate("utf-8").desc()
            ),
            "SELECT mytable.name FROM mytable "
            'ORDER BY mytable.name COLLATE "utf-8" DESC',
        )

    def test_operator_precedence_collate_6(self):
        self.assert_compile(
            select(self.table1.c.name).order_by(
                self.table1.c.name.collate("utf-8").desc().nulls_last()
            ),
            "SELECT mytable.name FROM mytable "
            'ORDER BY mytable.name COLLATE "utf-8" DESC NULLS LAST',
        )

    def test_operator_precedence_collate_7(self):
        self.assert_compile(
            select(self.table1.c.name).order_by(
                self.table1.c.name.collate("utf-8").asc()
            ),
            "SELECT mytable.name FROM mytable "
            'ORDER BY mytable.name COLLATE "utf-8" ASC',
        )

    def test_commutative_operators(self):
        class Nonsensical(String):
            operator_classes = OperatorClass.STRING | OperatorClass.NUMERIC

        self.assert_compile(
            literal("x", Nonsensical)
            + literal("y", Nonsensical) * literal("q", Nonsensical),
            ":param_1 || :param_2 * :param_3",
        )

    def test_op_operators(self):
        self.assert_compile(
            self.table1.select().where(
                self.table1.c.myid.op("hoho")(12) == 14
            ),
            "SELECT mytable.myid, mytable.name, mytable.description FROM "
            "mytable WHERE (mytable.myid hoho :myid_1) = :param_1",
        )

    def test_op_operators_comma_precedence(self):
        self.assert_compile(
            func.foo(self.table1.c.myid.op("hoho")(12)),
            "foo(mytable.myid hoho :myid_1)",
        )

    def test_op_operators_comparison_precedence(self):
        self.assert_compile(
            self.table1.c.myid.op("hoho")(12) == 5,
            "(mytable.myid hoho :myid_1) = :param_1",
        )

    def test_op_operators_custom_precedence(self):
        op1 = self.table1.c.myid.op("hoho", precedence=5)
        op2 = op1(5).op("lala", precedence=4)(4)
        op3 = op1(5).op("lala", precedence=6)(4)

        self.assert_compile(op2, "mytable.myid hoho :myid_1 lala :param_1")
        self.assert_compile(op3, "(mytable.myid hoho :myid_1) lala :param_1")

    def test_bitwise_not_precedence(self):
        op1 = operators.bitwise_not_op
        c = self.table1.c.myid
        op2 = op1(c).op("lala", precedence=7)(4)
        op3 = op1(c).op("lala", precedence=9)(4)

        self.assert_compile(op2, "~mytable.myid lala :param_1")
        self.assert_compile(op3, "(~mytable.myid) lala :param_1")

    @testing.combinations(
        ("xor", operators.bitwise_xor_op, "^"),
        ("or", operators.bitwise_or_op, "|"),
        ("and", operators.bitwise_and_op, "&"),
        ("lshift", operators.bitwise_lshift_op, "<<"),
        ("rshift", operators.bitwise_rshift_op, ">>"),
        id_="iaa",
    )
    def test_bitwise_op_precedence(self, py_op, sql_op):
        c = self.table1.c.myid
        op1 = py_op(c, 5).op("lala", precedence=6)(4)
        op2 = py_op(c, 5).op("lala", precedence=8)(4)

        self.assert_compile(
            op1, f"mytable.myid {sql_op} :myid_1 lala :param_1"
        )
        self.assert_compile(
            op2, f"(mytable.myid {sql_op} :myid_1) lala :param_1"
        )

    def test_is_eq_precedence_flat(self):
        self.assert_compile(
            (self.table1.c.name == null())
            != (self.table1.c.description == null()),
            "(mytable.name IS NULL) != (mytable.description IS NULL)",
        )


class OperatorAssociativityTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    def test_associativity_1(self):
        f = column("f")
        self.assert_compile(f - f, "f - f")

    def test_associativity_2(self):
        f = column("f")
        self.assert_compile(f - f - f, "(f - f) - f")

    def test_associativity_3(self):
        f = column("f")
        self.assert_compile((f - f) - f, "(f - f) - f")

    def test_associativity_4(self):
        f = column("f")
        self.assert_compile((f - f).label("foo") - f, "(f - f) - f")

    def test_associativity_5(self):
        f = column("f")
        self.assert_compile(f - (f - f), "f - (f - f)")

    def test_associativity_6(self):
        f = column("f")
        self.assert_compile(f - (f - f).label("foo"), "f - (f - f)")

    def test_associativity_7(self):
        f = column("f")
        # because - less precedent than /
        self.assert_compile(f / (f - f), "f / CAST((f - f) AS NUMERIC)")

    def test_associativity_8(self):
        f = column("f")
        self.assert_compile(
            f / (f - f).label("foo"), "f / CAST((f - f) AS NUMERIC)"
        )

    def test_associativity_9(self):
        f = column("f")
        self.assert_compile(f / f - f, "f / CAST(f AS NUMERIC) - f")

    def test_associativity_10(self):
        f = column("f")
        self.assert_compile((f / f) - f, "f / CAST(f AS NUMERIC) - f")

    def test_associativity_11(self):
        f = column("f")
        self.assert_compile(
            (f / f).label("foo") - f, "f / CAST(f AS NUMERIC) - f"
        )

    def test_associativity_12(self):
        f = column("f")
        # because / more precedent than -
        self.assert_compile(f - (f / f), "f - f / CAST(f AS NUMERIC)")

    def test_associativity_13(self):
        f = column("f")
        self.assert_compile(
            f - (f / f).label("foo"), "f - f / CAST(f AS NUMERIC)"
        )

    def test_associativity_14(self):
        f = column("f")
        self.assert_compile(f - f / f, "f - f / CAST(f AS NUMERIC)")

    def test_associativity_15(self):
        f = column("f")
        self.assert_compile((f - f) / f, "(f - f) / CAST(f AS NUMERIC)")

    def test_associativity_16(self):
        f = column("f")
        self.assert_compile(
            ((f - f) / f) - f, "(f - f) / CAST(f AS NUMERIC) - f"
        )

    def test_associativity_17(self):
        f = column("f")
        # - lower precedence than /
        self.assert_compile(
            (f - f) / (f - f), "(f - f) / CAST((f - f) AS NUMERIC)"
        )

    def test_associativity_18(self):
        f = column("f")
        # / higher precedence than -
        self.assert_compile(
            (f / f) - (f / f),
            "f / CAST(f AS NUMERIC) - f / CAST(f AS NUMERIC)",
        )

    def test_associativity_19(self):
        f = column("f")
        self.assert_compile(
            (f / f) - (f - f), "f / CAST(f AS NUMERIC) - (f - f)"
        )

    def test_associativity_20(self):
        f = column("f")
        self.assert_compile(
            (f / f) / (f - f),
            "(f / CAST(f AS NUMERIC)) / CAST((f - f) AS NUMERIC)",
        )

    def test_associativity_21(self):
        f = column("f")
        self.assert_compile(
            f / (f / (f - f)),
            "f / CAST((f / CAST((f - f) AS NUMERIC)) AS NUMERIC)",
        )

    def test_associativity_22(self):
        f = column("f")
        self.assert_compile((f == f) == f, "(f = f) = f")

    def test_associativity_23(self):
        f = column("f")
        self.assert_compile((f != f) != f, "(f != f) != f")


class IsDistinctFromTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table("mytable", column("myid", Integer))

    def test_is_distinct_from(self):
        self.assert_compile(
            self.table1.c.myid.is_distinct_from(1),
            "mytable.myid IS DISTINCT FROM :myid_1",
        )

    def test_is_distinct_from_sqlite(self):
        self.assert_compile(
            self.table1.c.myid.is_distinct_from(1),
            "mytable.myid IS NOT ?",
            dialect=sqlite.dialect(),
        )

    def test_is_distinct_from_postgresql(self):
        self.assert_compile(
            self.table1.c.myid.is_distinct_from(1),
            "mytable.myid IS DISTINCT FROM %(myid_1)s",
            dialect=postgresql.dialect(),
        )

    def test_not_is_distinct_from(self):
        self.assert_compile(
            ~self.table1.c.myid.is_distinct_from(1),
            "mytable.myid IS NOT DISTINCT FROM :myid_1",
        )

    def test_not_is_distinct_from_postgresql(self):
        self.assert_compile(
            ~self.table1.c.myid.is_distinct_from(1),
            "mytable.myid IS NOT DISTINCT FROM %(myid_1)s",
            dialect=postgresql.dialect(),
        )

    def test_is_not_distinct_from(self):
        self.assert_compile(
            self.table1.c.myid.is_not_distinct_from(1),
            "mytable.myid IS NOT DISTINCT FROM :myid_1",
        )

    def test_is_not_distinct_from_sqlite(self):
        self.assert_compile(
            self.table1.c.myid.is_not_distinct_from(1),
            "mytable.myid IS ?",
            dialect=sqlite.dialect(),
        )

    def test_is_not_distinct_from_postgresql(self):
        self.assert_compile(
            self.table1.c.myid.is_not_distinct_from(1),
            "mytable.myid IS NOT DISTINCT FROM %(myid_1)s",
            dialect=postgresql.dialect(),
        )

    def test_not_is_not_distinct_from(self):
        self.assert_compile(
            ~self.table1.c.myid.is_not_distinct_from(1),
            "mytable.myid IS DISTINCT FROM :myid_1",
        )

    def test_not_is_not_distinct_from_postgresql(self):
        self.assert_compile(
            ~self.table1.c.myid.is_not_distinct_from(1),
            "mytable.myid IS DISTINCT FROM %(myid_1)s",
            dialect=postgresql.dialect(),
        )


class InTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table("mytable", column("myid", Integer))
    table2 = table(
        "myothertable", column("otherid", Integer), column("othername", String)
    )

    def test_in_1(self):
        self.assert_compile(
            self.table1.c.myid.in_(["a"]),
            "mytable.myid IN (__[POSTCOMPILE_myid_1])",
            checkparams={"myid_1": ["a"]},
        )

    def test_in_2(self):
        self.assert_compile(
            ~self.table1.c.myid.in_(["a"]),
            "(mytable.myid NOT IN (__[POSTCOMPILE_myid_1]))",
            checkparams={"myid_1": ["a"]},
        )

    def test_in_3(self):
        self.assert_compile(
            self.table1.c.myid.in_(["a", "b"]),
            "mytable.myid IN (__[POSTCOMPILE_myid_1])",
            checkparams={"myid_1": ["a", "b"]},
        )

    def test_in_4(self):
        self.assert_compile(
            self.table1.c.myid.in_(iter(["a", "b"])),
            "mytable.myid IN (__[POSTCOMPILE_myid_1])",
            checkparams={"myid_1": ["a", "b"]},
        )

    def test_in_5(self):
        self.assert_compile(
            self.table1.c.myid.in_([literal("a")]),
            "mytable.myid IN (:param_1)",
        )

    def test_in_6(self):
        self.assert_compile(
            self.table1.c.myid.in_([literal("a"), "b"]),
            "mytable.myid IN (:param_1, :myid_1)",
        )

    def test_in_7(self):
        self.assert_compile(
            self.table1.c.myid.in_([literal("a"), literal("b")]),
            "mytable.myid IN (:param_1, :param_2)",
        )

    def test_in_8(self):
        self.assert_compile(
            self.table1.c.myid.in_(["a", literal("b")]),
            "mytable.myid IN (:myid_1, :param_1)",
        )

    def test_in_9(self):
        self.assert_compile(
            self.table1.c.myid.in_([literal(1) + "a"]),
            "mytable.myid IN (:param_1 + :param_2)",
        )

    def test_in_10(self):
        # when non-literal expressions are present we still need to do the
        # old way where we render up front
        self.assert_compile(
            self.table1.c.myid.in_([literal("a") + "a", "b"]),
            "mytable.myid IN (:param_1 || :param_2, :myid_1)",
        )

    def test_in_11(self):
        self.assert_compile(
            self.table1.c.myid.in_(
                [literal("a") + literal("a"), literal("b")]
            ),
            "mytable.myid IN (:param_1 || :param_2, :param_3)",
        )

    def test_in_12(self):
        self.assert_compile(
            self.table1.c.myid.in_([1, literal(3) + 4]),
            "mytable.myid IN (:myid_1, :param_1 + :param_2)",
        )

    def test_in_13(self):
        self.assert_compile(
            self.table1.c.myid.in_([literal("a") < "b"]),
            "mytable.myid IN (:param_1 < :param_2)",
        )

    def test_in_14(self):
        self.assert_compile(
            self.table1.c.myid.in_([self.table1.c.myid]),
            "mytable.myid IN (mytable.myid)",
        )

    def test_in_14_5(self):
        """test #12019"""
        self.assert_compile(
            self.table1.c.myid.in_(
                [ColExpressionDuckTypeOnly(self.table1.c.myid)]
            ),
            "mytable.myid IN (mytable.myid)",
        )

    def test_in_15(self):
        self.assert_compile(
            self.table1.c.myid.in_(["a", self.table1.c.myid]),
            "mytable.myid IN (:myid_1, mytable.myid)",
        )

    def test_in_16(self):
        self.assert_compile(
            self.table1.c.myid.in_([literal("a"), self.table1.c.myid]),
            "mytable.myid IN (:param_1, mytable.myid)",
        )

    def test_in_17(self):
        self.assert_compile(
            self.table1.c.myid.in_([literal("a"), self.table1.c.myid + "a"]),
            "mytable.myid IN (:param_1, mytable.myid + :myid_1)",
        )

    def test_in_18(self):
        self.assert_compile(
            self.table1.c.myid.in_([literal(1), "a" + self.table1.c.myid]),
            "mytable.myid IN (:param_1, :myid_1 + mytable.myid)",
        )

    def test_in_19(self):
        self.assert_compile(
            self.table1.c.myid.in_([1, 2, 3]),
            "mytable.myid IN (__[POSTCOMPILE_myid_1])",
            checkparams={"myid_1": [1, 2, 3]},
        )

    def test_in_20(self):
        self.assert_compile(
            self.table1.c.myid.in_(select(self.table2.c.otherid)),
            "mytable.myid IN (SELECT myothertable.otherid FROM myothertable)",
        )

    def test_in_21(self):
        self.assert_compile(
            ~self.table1.c.myid.in_(select(self.table2.c.otherid)),
            "(mytable.myid NOT IN "
            "(SELECT myothertable.otherid FROM myothertable))",
        )

    def test_in_22(self):
        self.assert_compile(
            self.table1.c.myid.in_(
                text("SELECT myothertable.otherid FROM myothertable")
            ),
            "mytable.myid IN (SELECT myothertable.otherid "
            "FROM myothertable)",
        )

    def test_in_24(self):
        self.assert_compile(
            select(self.table1.c.myid.in_(select(self.table2.c.otherid))),
            "SELECT mytable.myid IN (SELECT myothertable.otherid "
            "FROM myothertable) AS anon_1 FROM mytable",
        )

    def test_in_25(self):
        self.assert_compile(
            select(
                self.table1.c.myid.in_(
                    select(self.table2.c.otherid).scalar_subquery()
                )
            ),
            "SELECT mytable.myid IN (SELECT myothertable.otherid "
            "FROM myothertable) AS anon_1 FROM mytable",
        )

    def test_in_26(self):
        self.assert_compile(
            self.table1.c.myid.in_(
                union(
                    select(self.table1.c.myid).where(self.table1.c.myid == 5),
                    select(self.table1.c.myid).where(self.table1.c.myid == 12),
                )
            ),
            "mytable.myid IN ("
            "SELECT mytable.myid FROM mytable WHERE mytable.myid = :myid_1 "
            "UNION SELECT mytable.myid FROM mytable "
            "WHERE mytable.myid = :myid_2)",
        )

    def test_in_27(self):
        # test that putting a select in an IN clause does not
        # blow away its ORDER BY clause
        self.assert_compile(
            select(self.table1, self.table2)
            .where(
                self.table2.c.otherid.in_(
                    select(self.table2.c.otherid)
                    .order_by(self.table2.c.othername)
                    .limit(10)
                    .correlate(False),
                )
            )
            .select_from(
                self.table1.join(
                    self.table2,
                    self.table1.c.myid == self.table2.c.otherid,
                )
            )
            .order_by(self.table1.c.myid),
            "SELECT mytable.myid, "
            "myothertable.otherid, myothertable.othername FROM mytable "
            "JOIN myothertable ON mytable.myid = myothertable.otherid "
            "WHERE myothertable.otherid IN (SELECT myothertable.otherid "
            "FROM myothertable ORDER BY myothertable.othername "
            "LIMIT :param_1) ORDER BY mytable.myid",
            {"param_1": 10},
        )

    def test_in_28(self):
        """revised to test #12314"""
        self.assert_compile(
            self.table1.c.myid.in_([None]),
            "mytable.myid IN (__[POSTCOMPILE_myid_1])",
        )

    @testing.combinations(
        [1, 2, None, 3],
        [None, None, None],
        [None, 2, 3, 3],
    )
    def test_in_null_combinations(self, expr):
        """test #12314"""

        self.assert_compile(
            self.table1.c.myid.in_(expr),
            "mytable.myid IN (__[POSTCOMPILE_myid_1])",
        )

    @testing.combinations(True, False)
    def test_in_29(self, is_in):
        a, b, c = (
            column("a", Integer),
            column("b", String),
            column("c", LargeBinary),
        )
        t1 = tuple_(a, b, c)
        expr = t1.in_([(3, "hi", b"there"), (4, "Q", b"P")])
        if not is_in:
            expr = ~expr

        if is_in:
            self.assert_compile(
                expr,
                "(a, b, c) %s (__[POSTCOMPILE_param_1])"
                % ("IN" if is_in else "NOT IN"),
                checkparams={"param_1": [(3, "hi", b"there"), (4, "Q", b"P")]},
            )
            self.assert_compile(
                expr,
                "(a, b, c) %s ((3, 'hi', 'there'), (4, 'Q', 'P'))"
                % ("IN" if is_in else "NOT IN"),
                literal_binds=True,
            )
        else:
            self.assert_compile(
                expr,
                "((a, b, c) NOT IN (__[POSTCOMPILE_param_1]))",
                checkparams={"param_1": [(3, "hi", b"there"), (4, "Q", b"P")]},
            )
            self.assert_compile(
                expr,
                "((a, b, c) NOT IN ((3, 'hi', 'there'), (4, 'Q', 'P')))",
                literal_binds=True,
            )

    @testing.combinations(True, False, argnames="is_in")
    @testing.combinations(True, False, argnames="negate")
    def test_in_empty_tuple(self, is_in, negate):
        a, b, c = (
            column("a", Integer),
            column("b", String),
            column("c", LargeBinary),
        )
        t1 = tuple_(a, b, c)

        if negate:
            expr = ~t1.not_in([]) if is_in else ~t1.in_([])
        else:
            expr = t1.in_([]) if is_in else t1.not_in([])

        if is_in:
            self.assert_compile(
                expr,
                "(a, b, c) IN (__[POSTCOMPILE_param_1])",
                checkparams={"param_1": []},
            )
            self.assert_compile(
                expr,
                "(a, b, c) IN ((NULL, NULL, NULL)) AND (1 != 1)",
                literal_binds=True,
                dialect="default_enhanced",
            )
        else:
            self.assert_compile(
                expr,
                "((a, b, c) NOT IN (__[POSTCOMPILE_param_1]))",
                checkparams={"param_1": []},
            )
            self.assert_compile(
                expr,
                "((a, b, c) NOT IN ((NULL, NULL, NULL)) OR (1 = 1))",
                literal_binds=True,
                dialect="default_enhanced",
            )

    @testing.combinations(True, False, argnames="is_in")
    @testing.combinations(True, False, argnames="negate")
    def test_in_empty_single(self, is_in, negate):
        a = column("a", Integer)

        if negate:
            expr = ~a.not_in([]) if is_in else ~a.in_([])
        else:
            expr = a.in_([]) if is_in else a.not_in([])

        if is_in:
            self.assert_compile(
                expr,
                "a IN (__[POSTCOMPILE_a_1])",
                checkparams={"a_1": []},
            )
            self.assert_compile(
                expr,
                "a IN (NULL) AND (1 != 1)",
                literal_binds=True,
                dialect="default_enhanced",
            )
        else:
            self.assert_compile(
                expr,
                "(a NOT IN (__[POSTCOMPILE_a_1]))",
                checkparams={"a_1": []},
            )
            self.assert_compile(
                expr,
                "(a NOT IN (NULL) OR (1 = 1))",
                literal_binds=True,
                dialect="default_enhanced",
            )

    def test_in_self_plus_negated(self):
        a = column("a", Integer)

        expr1 = a.in_([5])
        expr2 = ~expr1

        stmt = and_(expr1, expr2)
        self.assert_compile(
            stmt,
            "a IN (__[POSTCOMPILE_a_1]) AND (a NOT IN (__[POSTCOMPILE_a_2]))",
        )
        self.assert_compile(
            stmt, "a IN (5) AND (a NOT IN (5))", literal_binds=True
        )

    def test_in_self_plus_negated_empty(self):
        a = column("a", Integer)

        expr1 = a.in_([])
        expr2 = ~expr1

        stmt = and_(expr1, expr2)
        self.assert_compile(
            stmt,
            "a IN (__[POSTCOMPILE_a_1]) AND (a NOT IN (__[POSTCOMPILE_a_2]))",
        )
        self.assert_compile(
            stmt,
            "a IN (NULL) AND (1 != 1) AND (a NOT IN (NULL) OR (1 = 1))",
            literal_binds=True,
        )

    def test_in_set(self):
        s = {1, 2, 3}
        self.assert_compile(
            self.table1.c.myid.in_(s),
            "mytable.myid IN (__[POSTCOMPILE_myid_1])",
            checkparams={"myid_1": list(s)},
        )

    def test_in_arbitrary_sequence(self):
        class MySeq:
            def __init__(self, d):
                self.d = d

            def __getitem__(self, idx):
                return self.d[idx]

            def __iter__(self):
                return iter(self.d)

        seq = MySeq([1, 2, 3])
        self.assert_compile(
            self.table1.c.myid.in_(seq),
            "mytable.myid IN (__[POSTCOMPILE_myid_1])",
            checkparams={"myid_1": [1, 2, 3]},
        )

    def test_scalar_subquery_wo_type(self):
        """test for :ticket:`6181`"""

        m = MetaData()
        t = Table("t", m, Column("a", Integer))

        # the scalar subquery of this will have no type; coercions will
        # want to call _with_binary_element_type(); that has to return
        # a scalar select
        req = select(column("scan"))

        self.assert_compile(
            select(t.c.a).where(t.c.a.in_(req)),
            "SELECT t.a FROM t WHERE t.a IN (SELECT scan)",
        )

    def test_type_inference_one(self):
        expr = column("q").in_([1, 2, 3])
        is_(expr.right.type._type_affinity, Integer)

        self.assert_compile(expr, "q IN (1, 2, 3)", literal_binds=True)

    def test_type_inference_two(self):
        expr = column("q").in_([])
        is_(expr.right.type, sqltypes.NULLTYPE)

        self.assert_compile(
            expr,
            "q IN (NULL) AND (1 != 1)",
            literal_binds=True,
            dialect="default_enhanced",
        )

    @testing.combinations(lambda v: v, lambda v: v.scalar_values())
    def test_in_values(self, scalar):
        t1, t2 = self.table1, self.table2
        v = scalar(values(t2.c.otherid).data([(1,), (42,)]))
        self.assert_compile(
            select(t1.c.myid.in_(v)),
            "SELECT mytable.myid IN (VALUES (:param_1), (:param_2)) "
            "AS anon_1 FROM mytable",
            params={"param_1": 1, "param_2": 42},
        )
        self.assert_compile(
            select(t1.c.myid.not_in(v)),
            "SELECT (mytable.myid NOT IN (VALUES (:param_1), (:param_2))) "
            "AS anon_1 FROM mytable",
            params={"param_1": 1, "param_2": 42},
        )


class MathOperatorTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table("mytable", column("myid", Integer))

    @testing.combinations(
        ("add", operator.add, "+"),
        ("mul", operator.mul, "*"),
        ("sub", operator.sub, "-"),
        ("mod", operator.mod, "%"),
        id_="iaa",
    )
    def test_math_op(self, py_op, sql_op):
        for lhs, rhs, res in (
            (5, self.table1.c.myid, ":myid_1 %s mytable.myid"),
            (5, literal(5), ":param_1 %s :param_2"),
            (self.table1.c.myid, "b", "mytable.myid %s :myid_1"),
            (self.table1.c.myid, literal(2.7), "mytable.myid %s :param_1"),
            (
                self.table1.c.myid,
                self.table1.c.myid,
                "mytable.myid %s mytable.myid",
            ),
            (literal(5), 8, ":param_1 %s :param_2"),
            (literal(6), self.table1.c.myid, ":param_1 %s mytable.myid"),
            (literal(7), literal(5.5), ":param_1 %s :param_2"),
        ):
            self.assert_compile(py_op(lhs, rhs), res % sql_op)

    def test_truediv_op_integer(self):
        self.assert_compile(
            5 / literal(5), ":param_1 / CAST(:param_2 AS NUMERIC)"
        )

    def test_floordiv_op_integer(self):
        self.assert_compile(5 // literal(5), ":param_1 / :param_2")

    def test_floordiv_op_numeric(self):
        self.assert_compile(5.10 // literal(5.5), "FLOOR(:param_1 / :param_2)")

    @testing.combinations(
        ("format", "mytable.myid %% %s"),
        ("qmark", "mytable.myid % ?"),
        ("named", "mytable.myid % :myid_1"),
        ("pyformat", "mytable.myid %% %(myid_1)s"),
    )
    def test_custom_op_percent_escaping(self, paramstyle, expected):
        expr = self.table1.c.myid.op("%")(5)

        self.assert_compile(
            expr,
            expected,
            dialect=default.DefaultDialect(paramstyle=paramstyle),
        )

    @testing.combinations(
        (operator.add,),
        (operator.mul,),
        (operator.sub,),
        (operator.floordiv),
    )
    def test_integer_integer_coercion_to_integer(self, op):
        expr = op(column("bar", Integer()), column("foo", Integer()))
        assert isinstance(expr.type, Integer)

    @testing.combinations(
        (operator.add,),
        (operator.mul,),
        (operator.sub,),
        (operator.truediv,),
    )
    def test_integer_numeric_coercion_to_numeric(self, op):
        expr = op(column("bar", Integer()), column("foo", Numeric(10, 2)))
        assert isinstance(expr.type, Numeric)
        expr = op(column("foo", Numeric(10, 2)), column("bar", Integer()))
        assert isinstance(expr.type, Numeric)

    def test_integer_truediv(self):
        expr = column("bar", Integer()) / column("foo", Integer)
        assert isinstance(expr.type, Numeric)

    def test_integer_floordiv(self):
        expr = column("bar", Integer()) // column("foo", Integer)
        assert isinstance(expr.type, Integer)

    def test_power_operator(self):
        expr = column("bar", Integer()) ** column("foo", Integer)
        self.assert_compile(expr, "pow(bar, foo)")
        expr = column("bar", Integer()) ** 42
        self.assert_compile(expr, "pow(bar, :pow_1)", {"pow_1": 42})
        expr = 99 ** column("bar", Integer())
        self.assert_compile(expr, "pow(:pow_1, bar)", {"pow_1": 42})


class ComparisonOperatorTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table("mytable", column("myid", String))

    def test_pickle_operators_one(self):
        clause = (
            (self.table1.c.myid == "12")
            & self.table1.c.myid.between("15", "20")
            & self.table1.c.myid.like("hoho")
        )
        eq_(str(clause), str(pickle.loads(pickle.dumps(clause))))

    def test_pickle_operators_two(self):
        clause = tuple_(1, 2, 3)
        eq_(str(clause), str(pickle.loads(pickle.dumps(clause))))

    @testing.combinations(Integer(), String(), JSON(), argnames="typ")
    @testing.variation("eval_first", [True, False])
    def test_pickle_comparator(self, typ, eval_first):
        """test #10213"""

        table1 = Table("t", MetaData(), Column("x", typ))
        t1 = table1.c.x

        if eval_first:
            t1.comparator

        t1p = pickle.loads(pickle.dumps(table1.c.x))

        is_not(t1p.comparator.__class__, NullType.Comparator)
        is_(t1.comparator.__class__, t1p.comparator.__class__)

    @testing.combinations(
        (operator.lt, "<", ">"),
        (operator.gt, ">", "<"),
        (operator.eq, "=", "="),
        (operator.ne, "!=", "!="),
        (operator.le, "<=", ">="),
        (operator.ge, ">=", "<="),
        id_="naa",
    )
    def test_comparison_op(self, py_op, fwd_op, rev_op):
        dt = datetime.datetime(2012, 5, 10, 15, 27, 18)
        for lhs, rhs, l_sql, r_sql in (
            ("a", self.table1.c.myid, ":myid_1", "mytable.myid"),
            ("a", literal("b"), ":param_2", ":param_1"),  # note swap!
            (self.table1.c.myid, "b", "mytable.myid", ":myid_1"),
            (self.table1.c.myid, literal("b"), "mytable.myid", ":param_1"),
            (
                self.table1.c.myid,
                self.table1.c.myid,
                "mytable.myid",
                "mytable.myid",
            ),
            (literal("a"), "b", ":param_1", ":param_2"),
            (literal("a"), self.table1.c.myid, ":param_1", "mytable.myid"),
            (literal("a"), literal("b"), ":param_1", ":param_2"),
            (dt, literal("b"), ":param_2", ":param_1"),
            (literal("b"), dt, ":param_1", ":param_2"),
        ):
            # the compiled clause should match either (e.g.):
            # 'a' < 'b' -or- 'b' > 'a'.
            compiled = str(py_op(lhs, rhs))
            fwd_sql = "%s %s %s" % (l_sql, fwd_op, r_sql)
            rev_sql = "%s %s %s" % (r_sql, rev_op, l_sql)

            self.assert_(
                compiled == fwd_sql or compiled == rev_sql,
                "\n'"
                + compiled
                + "'\n does not match\n'"
                + fwd_sql
                + "'\n or\n'"
                + rev_sql
                + "'",
            )


class NonZeroTest(fixtures.TestBase):
    def _raises(self, expr):
        assert_raises_message(
            TypeError,
            "Boolean value of this clause is not defined",
            bool,
            expr,
        )

    def _assert_true(self, expr):
        is_(bool(expr), True)

    def _assert_false(self, expr):
        is_(bool(expr), False)

    def test_column_identity_eq(self):
        c1 = column("c1")
        self._assert_true(c1 == c1)

    def test_column_identity_gt(self):
        c1 = column("c1")
        self._raises(c1 > c1)

    def test_column_compare_eq(self):
        c1, c2 = column("c1"), column("c2")
        self._assert_false(c1 == c2)

    def test_column_compare_gt(self):
        c1, c2 = column("c1"), column("c2")
        self._raises(c1 > c2)

    def test_binary_identity_eq(self):
        c1 = column("c1")
        expr = c1 > 5
        self._assert_true(expr == expr)

    def test_labeled_binary_identity_eq(self):
        c1 = column("c1")
        expr = (c1 > 5).label(None)
        self._assert_true(expr == expr)

    def test_annotated_binary_identity_eq(self):
        c1 = column("c1")
        expr1 = c1 > 5
        expr2 = expr1._annotate({"foo": "bar"})
        self._assert_true(expr1 == expr2)

    def test_labeled_binary_compare_gt(self):
        c1 = column("c1")
        expr1 = (c1 > 5).label(None)
        expr2 = (c1 > 5).label(None)
        self._assert_false(expr1 == expr2)


class NegationTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table("mytable", column("myid", Integer), column("name", String))

    @testing.combinations(
        (~literal(5, NullType), "NOT :param_1"),
        (~-literal(5, NullType), "NOT -:param_1"),
    )
    def test_nonsensical_negates(self, expr, expected):
        """exercise codepaths in the UnaryExpression._negate() method where the
        type is not BOOLEAN"""

        self.assert_compile(expr, expected)

    def test_negate_operators_1(self):
        for py_op, op in ((operator.neg, "-"), (operator.inv, "NOT ")):
            for expr, expected in (
                (self.table1.c.myid, "mytable.myid"),
                (literal(5, Integer), ":param_1"),
            ):
                self.assert_compile(py_op(expr), "%s%s" % (op, expected))

    def test_negate_operators_2(self):
        self.assert_compile(
            self.table1.select().where(
                (self.table1.c.myid != 12) & ~(self.table1.c.name == "john")
            ),
            "SELECT mytable.myid, mytable.name FROM "
            "mytable WHERE mytable.myid != :myid_1 "
            "AND mytable.name != :name_1",
        )

    def test_negate_operators_3(self):
        self.assert_compile(
            self.table1.select().where(
                (self.table1.c.myid != 12)
                & ~(self.table1.c.name.between("jack", "john"))
            ),
            "SELECT mytable.myid, mytable.name FROM "
            "mytable WHERE mytable.myid != :myid_1 AND "
            "mytable.name NOT BETWEEN :name_1 AND :name_2",
        )

    def test_negate_operators_4(self):
        self.assert_compile(
            self.table1.select().where(
                (self.table1.c.myid != 12)
                & ~and_(
                    self.table1.c.name == "john",
                    self.table1.c.name == "ed",
                    self.table1.c.name == "fred",
                )
            ),
            "SELECT mytable.myid, mytable.name FROM "
            "mytable WHERE mytable.myid != :myid_1 AND "
            "NOT (mytable.name = :name_1 AND mytable.name = :name_2 "
            "AND mytable.name = :name_3)",
        )

    def test_negate_operators_5(self):
        self.assert_compile(
            self.table1.select().where(
                (self.table1.c.myid != "12")
                & ~and_(
                    literal("somethingboolean", Boolean), literal("q", Boolean)
                )
            ),
            "SELECT mytable.myid, mytable.name FROM "
            "mytable WHERE mytable.myid != :myid_1 AND NOT "
            "(:param_1 = 1 AND :param_2 = 1)",
        )

    def test_negate_operators_6(self):
        self.assert_compile(
            self.table1.select().where(
                (self.table1.c.myid != "12")
                & ~literal("somethingboolean", Boolean)
            ),
            "SELECT mytable.myid, mytable.name FROM "
            "mytable WHERE mytable.myid != :myid_1 AND NOT :param_1",
            supports_native_boolean=True,
            use_default_dialect=True,
        )

    def test_negate_operator_type(self):
        is_((-self.table1.c.myid).type, self.table1.c.myid.type)

    def test_negate_operator_label(self):
        orig_expr = or_(
            self.table1.c.myid == 1, self.table1.c.myid == 2
        ).label("foo")
        expr = not_(orig_expr)
        isinstance(expr, Label)
        eq_(expr.name, "foo")
        is_not(expr, orig_expr)
        is_(expr._element.operator, operator.inv)  # e.g. and not false_

        self.assert_compile(
            expr,
            "NOT (mytable.myid = :myid_1 OR mytable.myid = :myid_2)",
            dialect=default.DefaultDialect(supports_native_boolean=False),
        )

    def test_negate_operator_self_group(self):
        orig_expr = or_(
            self.table1.c.myid == 1, self.table1.c.myid == 2
        ).self_group()
        expr = not_(orig_expr)
        is_not(expr, orig_expr)

        self.assert_compile(
            expr,
            "NOT (mytable.myid = :myid_1 OR mytable.myid = :myid_2)",
            dialect=default.DefaultDialect(supports_native_boolean=False),
        )

    def test_implicitly_boolean(self):
        # test for expressions that the database always considers as boolean
        # even if there is no boolean datatype.
        assert not self.table1.c.myid._is_implicitly_boolean
        assert (self.table1.c.myid == 5)._is_implicitly_boolean
        assert (self.table1.c.myid == 5).self_group()._is_implicitly_boolean
        assert (self.table1.c.myid == 5).label("x")._is_implicitly_boolean
        assert not_(self.table1.c.myid == 5)._is_implicitly_boolean
        assert or_(
            self.table1.c.myid == 5, self.table1.c.myid == 7
        )._is_implicitly_boolean
        assert not column("x", Boolean)._is_implicitly_boolean
        assert not (self.table1.c.myid + 5)._is_implicitly_boolean
        assert not not_(column("x", Boolean))._is_implicitly_boolean
        assert (
            not select(self.table1.c.myid)
            .scalar_subquery()
            ._is_implicitly_boolean
        )
        assert not text("x = y")._is_implicitly_boolean
        assert not literal_column("x = y")._is_implicitly_boolean

    def test_scalar_select(self):
        t = self.table1
        expr = select(t.c.myid).where(t.c.myid > 5).scalar_subquery()
        self.assert_compile(
            not_(expr),
            "NOT (SELECT mytable.myid FROM mytable "
            "WHERE mytable.myid > :myid_1)",
            params={"myid_1": 5},
        )

    def test_scalar_values(self):
        t = self.table1
        expr = values(t.c.myid).data([(7,), (42,)]).scalar_values()
        self.assert_compile(
            not_(expr),
            "NOT (VALUES (:param_1), (:param_2))",
            params={"param_1": 7, "param_2": 42},
        )


class LikeTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table("mytable", column("myid", String), column("name", String))

    def test_like_1(self):
        self.assert_compile(
            self.table1.c.myid.like("somstr"), "mytable.myid LIKE :myid_1"
        )

    def test_like_2(self):
        self.assert_compile(
            ~self.table1.c.myid.like("somstr"), "mytable.myid NOT LIKE :myid_1"
        )

    def test_like_3(self):
        self.assert_compile(
            self.table1.c.myid.like("somstr", escape="\\"),
            "mytable.myid LIKE :myid_1 ESCAPE '\\'",
        )

    def test_like_quote_escape(self):
        self.assert_compile(
            self.table1.c.myid.like("somstr", escape="'"),
            "mytable.myid LIKE :myid_1 ESCAPE ''''",
        )

    def test_like_4(self):
        self.assert_compile(
            ~self.table1.c.myid.like("somstr", escape="\\"),
            "mytable.myid NOT LIKE :myid_1 ESCAPE '\\'",
        )

    def test_like_5(self):
        self.assert_compile(
            self.table1.c.myid.ilike("somstr", escape="\\"),
            "lower(mytable.myid) LIKE lower(:myid_1) ESCAPE '\\'",
        )

    def test_like_6(self):
        self.assert_compile(
            ~self.table1.c.myid.ilike("somstr", escape="\\"),
            "lower(mytable.myid) NOT LIKE lower(:myid_1) ESCAPE '\\'",
        )

    def test_like_7(self):
        self.assert_compile(
            self.table1.c.myid.ilike("somstr", escape="\\"),
            "mytable.myid ILIKE %(myid_1)s ESCAPE '\\\\'",
            dialect=postgresql.dialect(),
        )

    def test_like_8(self):
        self.assert_compile(
            ~self.table1.c.myid.ilike("somstr", escape="\\"),
            "mytable.myid NOT ILIKE %(myid_1)s ESCAPE '\\\\'",
            dialect=postgresql.dialect(),
        )

    def test_like_9(self):
        self.assert_compile(
            self.table1.c.name.ilike("%something%"),
            "lower(mytable.name) LIKE lower(:name_1)",
        )

    def test_like_10(self):
        self.assert_compile(
            self.table1.c.name.ilike("%something%"),
            "mytable.name ILIKE %(name_1)s",
            dialect=postgresql.dialect(),
        )

    def test_like_11(self):
        self.assert_compile(
            ~self.table1.c.name.ilike("%something%"),
            "lower(mytable.name) NOT LIKE lower(:name_1)",
        )

    def test_like_12(self):
        self.assert_compile(
            ~self.table1.c.name.ilike("%something%"),
            "mytable.name NOT ILIKE %(name_1)s",
            dialect=postgresql.dialect(),
        )


class BetweenTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table("mytable", column("myid", Integer), column("name", String))

    def test_between_1(self):
        self.assert_compile(
            self.table1.c.myid.between(1, 2),
            "mytable.myid BETWEEN :myid_1 AND :myid_2",
        )

    def test_between_2(self):
        self.assert_compile(
            ~self.table1.c.myid.between(1, 2),
            "mytable.myid NOT BETWEEN :myid_1 AND :myid_2",
        )

    def test_between_3(self):
        self.assert_compile(
            self.table1.c.myid.between(1, 2, symmetric=True),
            "mytable.myid BETWEEN SYMMETRIC :myid_1 AND :myid_2",
        )

    def test_between_4(self):
        self.assert_compile(
            ~self.table1.c.myid.between(1, 2, symmetric=True),
            "mytable.myid NOT BETWEEN SYMMETRIC :myid_1 AND :myid_2",
        )

    def test_between_5(self):
        self.assert_compile(
            between(self.table1.c.myid, 1, 2, symmetric=True),
            "mytable.myid BETWEEN SYMMETRIC :myid_1 AND :myid_2",
        )

    def test_between_6(self):
        self.assert_compile(
            ~between(self.table1.c.myid, 1, 2, symmetric=True),
            "mytable.myid NOT BETWEEN SYMMETRIC :myid_1 AND :myid_2",
        )


class MatchTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    table1 = table("mytable", column("myid", String), column("name", String))

    def test_match_1(self):
        self.assert_compile(
            self.table1.c.myid.match("somstr"),
            "mytable.myid MATCH ?",
            dialect=sqlite.dialect(),
        )

    def test_match_2(self):
        self.assert_compile(
            self.table1.c.myid.match("somstr"),
            "MATCH (mytable.myid) AGAINST (%s IN BOOLEAN MODE)",
            dialect=mysql.dialect(),
        )

    def test_match_3(self):
        self.assert_compile(
            self.table1.c.myid.match("somstr"),
            "CONTAINS (mytable.myid, :myid_1)",
            dialect=mssql.dialect(),
        )

    def test_match_4(self):
        self.assert_compile(
            self.table1.c.myid.match("somstr"),
            "mytable.myid @@ plainto_tsquery(%(myid_1)s)",
            dialect=postgresql.dialect(),
        )

    def test_match_5(self):
        self.assert_compile(
            self.table1.c.myid.match("somstr"),
            "CONTAINS (mytable.myid, :myid_1)",
            dialect=oracle.dialect(),
        )

    def test_match_is_now_matchtype(self):
        expr = self.table1.c.myid.match("somstr")
        assert expr.type._type_affinity is MatchType()._type_affinity
        assert isinstance(expr.type, MatchType)

    def test_boolean_inversion_postgresql(self):
        self.assert_compile(
            ~self.table1.c.myid.match("somstr"),
            "NOT mytable.myid @@ plainto_tsquery(%(myid_1)s)",
            dialect=postgresql.dialect(),
        )

    def test_boolean_inversion_mysql(self):
        # because mysql doesn't have native boolean
        self.assert_compile(
            ~self.table1.c.myid.match("somstr"),
            "NOT MATCH (mytable.myid) AGAINST (%s IN BOOLEAN MODE)",
            dialect=mysql.dialect(),
        )

    def test_boolean_inversion_mssql(self):
        # because mssql doesn't have native boolean
        self.assert_compile(
            ~self.table1.c.myid.match("somstr"),
            "NOT CONTAINS (mytable.myid, :myid_1)",
            dialect=mssql.dialect(),
        )


class RegexpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    def setup_test(self):
        self.table = table(
            "mytable", column("myid", String), column("name", String)
        )

    def test_regexp_match(self):
        assert_raises_message(
            exc.CompileError,
            "default dialect does not support regular expressions",
            self.table.c.myid.regexp_match("pattern").compile,
            dialect=default.DefaultDialect(),
        )

    def test_not_regexp_match(self):
        assert_raises_message(
            exc.CompileError,
            "default dialect does not support regular expressions",
            (~self.table.c.myid.regexp_match("pattern")).compile,
            dialect=default.DefaultDialect(),
        )

    def test_regexp_replace(self):
        assert_raises_message(
            exc.CompileError,
            "default dialect does not support regular expression replacements",
            self.table.c.myid.regexp_replace("pattern", "rep").compile,
            dialect=default.DefaultDialect(),
        )


class RegexpTestStrCompiler(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default_enhanced"

    def setup_test(self):
        self.table = table(
            "mytable",
            column("myid", String),
            column("name", String),
            column("myinteger", Integer),
        )

    def test_regexp_match(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern"),
            "mytable.myid <regexp> :myid_1",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_match_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid <regexp> mytable.name",
            checkparams={},
        )

    def test_regexp_match_str(self):
        self.assert_compile(
            literal("string").regexp_match(self.table.c.name),
            ":param_1 <regexp> mytable.name",
            checkparams={"param_1": "string"},
        )

    def test_regexp_match_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid <regexp> :myid_1",
            checkparams={"myid_1": "pattern"},
        )

    def test_not_regexp_match(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern"),
            "mytable.myid <not regexp> :myid_1",
            checkparams={"myid_1": "pattern"},
        )

    def test_not_regexp_match_column(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match(self.table.c.name),
            "mytable.myid <not regexp> mytable.name",
            checkparams={},
        )

    def test_not_regexp_match_str(self):
        self.assert_compile(
            ~literal("string").regexp_match(self.table.c.name),
            ":param_1 <not regexp> mytable.name",
            checkparams={"param_1": "string"},
        )

    def test_not_regexp_match_flags(self):
        self.assert_compile(
            ~self.table.c.myid.regexp_match("pattern", flags="ig"),
            "mytable.myid <not regexp> :myid_1",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_replace(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace("pattern", "replacement"),
            "<regexp replace>(mytable.myid, :myid_1, :myid_2)",
            checkparams={"myid_1": "pattern", "myid_2": "replacement"},
        )

    def test_regexp_replace_column(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace("pattern", self.table.c.name),
            "<regexp replace>(mytable.myid, :myid_1, mytable.name)",
            checkparams={"myid_1": "pattern"},
        )

    def test_regexp_replace_column2(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(self.table.c.name, "replacement"),
            "<regexp replace>(mytable.myid, mytable.name, :myid_1)",
            checkparams={"myid_1": "replacement"},
        )

    def test_regexp_replace_string(self):
        self.assert_compile(
            literal("string").regexp_replace("pattern", self.table.c.name),
            "<regexp replace>(:param_1, :param_2, mytable.name)",
            checkparams={"param_2": "pattern", "param_1": "string"},
        )

    def test_regexp_replace_flags(self):
        self.assert_compile(
            self.table.c.myid.regexp_replace(
                "pattern", "replacement", flags="ig"
            ),
            "<regexp replace>(mytable.myid, :myid_1, :myid_2)",
            checkparams={"myid_1": "pattern", "myid_2": "replacement"},
        )

    def test_regexp_precedence_1(self):
        self.assert_compile(
            and_(
                self.table.c.myid.match("foo"),
                self.table.c.myid.regexp_match("xx"),
            ),
            "mytable.myid MATCH :myid_1 AND mytable.myid <regexp> :myid_2",
        )
        self.assert_compile(
            and_(
                self.table.c.myid.match("foo"),
                ~self.table.c.myid.regexp_match("xx"),
            ),
            "mytable.myid MATCH :myid_1 AND "
            "mytable.myid <not regexp> :myid_2",
        )
        self.assert_compile(
            and_(
                self.table.c.myid.match("foo"),
                self.table.c.myid.regexp_replace("xx", "yy"),
            ),
            "mytable.myid MATCH :myid_1 AND "
            "<regexp replace>(mytable.myid, :myid_2, :myid_3)",
        )

    def test_regexp_precedence_2(self):
        self.assert_compile(
            self.table.c.myinteger + self.table.c.myid.regexp_match("xx"),
            "mytable.myinteger + (mytable.myid <regexp> :myid_1)",
        )
        self.assert_compile(
            self.table.c.myinteger + ~self.table.c.myid.regexp_match("xx"),
            "mytable.myinteger + (mytable.myid <not regexp> :myid_1)",
        )
        self.assert_compile(
            self.table.c.myinteger
            + self.table.c.myid.regexp_replace("xx", "yy"),
            "mytable.myinteger + ("
            "<regexp replace>(mytable.myid, :myid_1, :myid_2))",
        )

    @testing.combinations(
        (lambda c, r: c.regexp_match(r), "<regexp>"),
        (lambda c, r: c.like(r), "LIKE"),
        (lambda c, r: ~c.regexp_match(r), "<not regexp>"),
        (lambda c, r: ~c.match(r), "NOT %s MATCH"),
        (lambda c, r: c.match(r), "MATCH"),
    )
    def test_all_match_precedence_against_concat(self, expr, expected):
        expr = testing.resolve_lambda(
            expr, c=self.table.c.myid, r=self.table.c.name + "some text"
        )

        if "%s" in expected:
            self.assert_compile(
                expr,
                f"{expected % ('mytable.myid', )} (mytable.name || :name_1)",
            )
        else:
            self.assert_compile(
                expr, f"mytable.myid {expected} (mytable.name || :name_1)"
            )


class ComposedLikeOperatorsTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    def test_contains(self):
        self.assert_compile(
            column("x").contains("y"),
            "x LIKE '%' || :x_1 || '%'",
            checkparams={"x_1": "y"},
        )

    def test_contains_pg(self):
        self.assert_compile(
            column("x").contains("y"),
            "x LIKE '%%' || %(x_1)s || '%%'",
            checkparams={"x_1": "y"},
            dialect="postgresql",
        )

    def test_contains_encoded(self):
        self.assert_compile(
            column("x").contains(b"y"),
            "x LIKE '%' || :x_1 || '%'",
            checkparams={"x_1": b"y"},
        )

    def test_not_contains_encoded(self):
        self.assert_compile(
            ~column("x").contains(b"y"),
            "x NOT LIKE '%' || :x_1 || '%'",
            checkparams={"x_1": b"y"},
        )

    def test_contains_encoded_mysql(self):
        self.assert_compile(
            column("x").contains(b"y"),
            "x LIKE concat('%%', %s, '%%')",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_not_contains_encoded_mysql(self):
        self.assert_compile(
            ~column("x").contains(b"y"),
            "x NOT LIKE concat('%%', %s, '%%')",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_not_contains_pg(self):
        self.assert_compile(
            ~column("x").contains(b"y"),
            "x NOT LIKE '%%' || %(x_1)s || '%%'",
            checkparams={"x_1": b"y"},
            dialect="postgresql",
        )

    def test_contains_escape(self):
        self.assert_compile(
            column("x").contains("a%b_c", escape="\\"),
            "x LIKE '%' || :x_1 || '%' ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_contains_autoescape(self):
        self.assert_compile(
            column("x").contains("a%b_c/d", autoescape=True),
            "x LIKE '%' || :x_1 || '%' ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_contains_literal(self):
        self.assert_compile(
            column("x").contains(literal_column("y")),
            "x LIKE '%' || y || '%'",
            checkparams={},
        )

    def test_contains_text(self):
        self.assert_compile(
            column("x").contains(text("y")),
            "x LIKE '%' || y || '%'",
            checkparams={},
        )

    def test_not_contains(self):
        self.assert_compile(
            ~column("x").contains("y"),
            "x NOT LIKE '%' || :x_1 || '%'",
            checkparams={"x_1": "y"},
        )

    def test_not_contains_escape(self):
        self.assert_compile(
            ~column("x").contains("a%b_c", escape="\\"),
            "x NOT LIKE '%' || :x_1 || '%' ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_not_contains_autoescape(self):
        self.assert_compile(
            ~column("x").contains("a%b_c/d", autoescape=True),
            "x NOT LIKE '%' || :x_1 || '%' ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_contains_concat(self):
        self.assert_compile(
            column("x").contains("y"),
            "x LIKE concat('%%', %s, '%%')",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_not_contains_concat(self):
        self.assert_compile(
            ~column("x").contains("y"),
            "x NOT LIKE concat('%%', %s, '%%')",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_contains_literal_concat(self):
        self.assert_compile(
            column("x").contains(literal_column("y")),
            "x LIKE concat('%%', y, '%%')",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_contains_text_concat(self):
        self.assert_compile(
            column("x").contains(text("y")),
            "x LIKE concat('%%', y, '%%')",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_icontains(self):
        """
        case insensitive contains method
        """
        self.assert_compile(
            column("x").icontains("y"),
            "lower(x) LIKE '%' || lower(:x_1) || '%'",
            checkparams={"x_1": "y"},
        )

    def test_icontains_pg(self):
        """
        case insensitive contains method
        """
        self.assert_compile(
            column("x").icontains("y"),
            "x ILIKE '%%' || %(x_1)s || '%%'",
            checkparams={"x_1": "y"},
            dialect="postgresql",
        )

    def test_icontains_encoded(self):
        self.assert_compile(
            column("x").icontains(b"y"),
            "lower(x) LIKE '%' || lower(:x_1) || '%'",
            checkparams={"x_1": b"y"},
        )

    def test_not_icontains_encoded(self):
        self.assert_compile(
            ~column("x").icontains(b"y"),
            "lower(x) NOT LIKE '%' || lower(:x_1) || '%'",
            checkparams={"x_1": b"y"},
        )

    def test_icontains_encoded_mysql(self):
        self.assert_compile(
            column("x").icontains(b"y"),
            "lower(x) LIKE concat('%%', lower(%s), '%%')",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_not_icontains_encoded_mysql(self):
        self.assert_compile(
            ~column("x").icontains(b"y"),
            "lower(x) NOT LIKE concat('%%', lower(%s), '%%')",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_not_icontains(self):
        """
        same as test_icontains but negate the statement
        """
        self.assert_compile(
            ~column("x").icontains("y"),
            "lower(x) NOT LIKE '%' || lower(:x_1) || '%'",
            checkparams={"x_1": "y"},
        )

    def test_not_icontains_pg(self):
        """
        same as test_icontains but negate the statement
        """
        self.assert_compile(
            ~column("x").icontains("y"),
            "x NOT ILIKE '%%' || %(x_1)s || '%%'",
            checkparams={"x_1": "y"},
            dialect="postgresql",
        )

    def test_icontains_escape(self):
        """
        render same SQL statement as icontains but include the ESCAPE
        keyword to establish that character as the escape character
        """
        self.assert_compile(
            column("x").icontains("a%b_c", escape="\\"),
            "lower(x) LIKE '%' || lower(:x_1) || '%' ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_not_icontains_escape(self):
        """
        same as test_icontains_escape but negate the statement
        """
        self.assert_compile(
            ~column("x").icontains("a%b_c", escape="\\"),
            "lower(x) NOT LIKE '%' || lower(:x_1) || '%' ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_icontains_autoescape(self):
        """
        apply the escape character to all occurrences of "%", "_" and
        the escape character itself
        """
        self.assert_compile(
            column("x").icontains("a%b_c/d", autoescape=True),
            "lower(x) LIKE '%' || lower(:x_1) || '%' ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_icontains_autoescape_custom_escape(self):
        """
        set an escape character to all occurrences of "%", "_" and
        the escape character itself
        """
        self.assert_compile(
            column("x").icontains("foo%bar^bat", escape="^", autoescape=True),
            "lower(x) LIKE '%' || lower(:x_1) || '%' ESCAPE '^'",
            checkparams={"x_1": "foo^%bar^^bat"},
        )

    def test_not_icontains_autoescape(self):
        """
        same as test_icontains_autoescape but negate the statement
        """
        self.assert_compile(
            ~column("x").icontains("a%b_c/d", autoescape=True),
            "lower(x) NOT LIKE '%' || lower(:x_1) || '%' ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_icontains_literal(self):
        self.assert_compile(
            column("x").icontains(literal_column("y")),
            "lower(x) LIKE '%' || lower(y) || '%'",
            checkparams={},
        )

    def test_icontains_text(self):
        self.assert_compile(
            column("x").icontains(text("y")),
            "lower(x) LIKE '%' || lower(y) || '%'",
            checkparams={},
        )

    def test_icontains_concat(self):
        self.assert_compile(
            column("x").icontains("y"),
            "lower(x) LIKE concat('%%', lower(%s), '%%')",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_not_icontains_concat(self):
        self.assert_compile(
            ~column("x").icontains("y"),
            "lower(x) NOT LIKE concat('%%', lower(%s), '%%')",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_icontains_literal_concat(self):
        self.assert_compile(
            column("x").icontains(literal_column("y")),
            "lower(x) LIKE concat('%%', lower(y), '%%')",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_icontains_text_concat(self):
        self.assert_compile(
            column("x").icontains(text("y")),
            "lower(x) LIKE concat('%%', lower(y), '%%')",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_like(self):
        self.assert_compile(
            column("x").like("y"), "x LIKE :x_1", checkparams={"x_1": "y"}
        )

    def test_like_escape(self):
        self.assert_compile(
            column("x").like("a%b_c", escape="\\"),
            "x LIKE :x_1 ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_like_escape_empty(self):
        self.assert_compile(
            column("x").like("y", escape=""),
            "x LIKE :x_1 ESCAPE ''",
            checkparams={"x_1": "y"},
        )

    def test_ilike(self):
        self.assert_compile(
            column("x").ilike("y"),
            "lower(x) LIKE lower(:x_1)",
            checkparams={"x_1": "y"},
        )

    def test_ilike_escape(self):
        self.assert_compile(
            column("x").ilike("a%b_c", escape="\\"),
            "lower(x) LIKE lower(:x_1) ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_ilike_escape_empty(self):
        self.assert_compile(
            column("x").ilike("y", escape=""),
            "lower(x) LIKE lower(:x_1) ESCAPE ''",
            checkparams={"x_1": "y"},
        )

    def test_not_like(self):
        self.assert_compile(
            column("x").not_like("y"),
            "x NOT LIKE :x_1",
            checkparams={"x_1": "y"},
        )

    def test_not_like_escape(self):
        self.assert_compile(
            column("x").not_like("a%b_c", escape="\\"),
            "x NOT LIKE :x_1 ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_not_like_escape_empty(self):
        self.assert_compile(
            column("x").not_like("y", escape=""),
            "x NOT LIKE :x_1 ESCAPE ''",
            checkparams={"x_1": "y"},
        )

    def test_not_ilike(self):
        self.assert_compile(
            column("x").not_ilike("y"),
            "lower(x) NOT LIKE lower(:x_1)",
            checkparams={"x_1": "y"},
        )

    def test_not_ilike_escape(self):
        self.assert_compile(
            column("x").not_ilike("a%b_c", escape="\\"),
            "lower(x) NOT LIKE lower(:x_1) ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_not_ilike_escape_empty(self):
        self.assert_compile(
            column("x").not_ilike("y", escape=""),
            "lower(x) NOT LIKE lower(:x_1) ESCAPE ''",
            checkparams={"x_1": "y"},
        )

    def test_startswith(self):
        self.assert_compile(
            column("x").startswith("y"),
            "x LIKE :x_1 || '%'",
            checkparams={"x_1": "y"},
        )

    def test_startswith_escape(self):
        self.assert_compile(
            column("x").startswith("a%b_c", escape="\\"),
            "x LIKE :x_1 || '%' ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_startswith_autoescape(self):
        self.assert_compile(
            column("x").startswith("a%b_c/d", autoescape=True),
            "x LIKE :x_1 || '%' ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_startswith_autoescape_custom_escape(self):
        self.assert_compile(
            column("x").startswith("a%b_c/d^e", autoescape=True, escape="^"),
            "x LIKE :x_1 || '%' ESCAPE '^'",
            checkparams={"x_1": "a^%b^_c/d^^e"},
        )

    def test_startswith_encoded(self):
        self.assert_compile(
            column("x").startswith(b"y"),
            "x LIKE :x_1 || '%'",
            checkparams={"x_1": b"y"},
        )

    def test_startswith_encoded_mysql(self):
        self.assert_compile(
            column("x").startswith(b"y"),
            "x LIKE concat(%s, '%%')",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_not_startswith_encoded(self):
        self.assert_compile(
            ~column("x").startswith(b"y"),
            "x NOT LIKE :x_1 || '%'",
            checkparams={"x_1": b"y"},
        )

    def test_not_startswith_encoded_mysql(self):
        self.assert_compile(
            ~column("x").startswith(b"y"),
            "x NOT LIKE concat(%s, '%%')",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_not_startswith(self):
        self.assert_compile(
            ~column("x").startswith("y"),
            "x NOT LIKE :x_1 || '%'",
            checkparams={"x_1": "y"},
        )

    def test_not_startswith_escape(self):
        self.assert_compile(
            ~column("x").startswith("a%b_c", escape="\\"),
            "x NOT LIKE :x_1 || '%' ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_not_startswith_autoescape(self):
        self.assert_compile(
            ~column("x").startswith("a%b_c/d", autoescape=True),
            "x NOT LIKE :x_1 || '%' ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_startswith_literal(self):
        self.assert_compile(
            column("x").startswith(literal_column("y")),
            "x LIKE y || '%'",
            checkparams={},
        )

    def test_startswith_text(self):
        self.assert_compile(
            column("x").startswith(text("y")),
            "x LIKE y || '%'",
            checkparams={},
        )

    def test_startswith_concat(self):
        self.assert_compile(
            column("x").startswith("y"),
            "x LIKE concat(%s, '%%')",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_not_startswith_concat(self):
        self.assert_compile(
            ~column("x").startswith("y"),
            "x NOT LIKE concat(%s, '%%')",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_startswith_literal_mysql(self):
        self.assert_compile(
            column("x").startswith(literal_column("y")),
            "x LIKE concat(y, '%%')",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_startswith_text_mysql(self):
        self.assert_compile(
            column("x").startswith(text("y")),
            "x LIKE concat(y, '%%')",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_istartswith(self):
        """
        case insensitive startswith method
        """
        self.assert_compile(
            column("x").istartswith("y"),
            "lower(x) LIKE lower(:x_1) || '%'",
            checkparams={"x_1": "y"},
        )

    def test_istartswith_pg(self):
        """
        case insensitive startswith method
        """
        self.assert_compile(
            column("x").istartswith("y"),
            "x ILIKE %(x_1)s || '%%'",
            checkparams={"x_1": "y"},
            dialect="postgresql",
        )

    def test_not_istartswith(self):
        """
        same as test_istartswith but negate the statement
        """
        self.assert_compile(
            ~column("x").istartswith("y"),
            "lower(x) NOT LIKE lower(:x_1) || '%'",
            checkparams={"x_1": "y"},
        )

    def test_not_istartswith_pg(self):
        """
        same as test_istartswith but negate the statement
        """
        self.assert_compile(
            ~column("x").istartswith("y"),
            "x NOT ILIKE %(x_1)s || '%%'",
            checkparams={"x_1": "y"},
            dialect="postgresql",
        )

    def test_istartswith_escape(self):
        """
        render same SQL statement as istartswith but include the ESCAPE
        keyword to establish that character as the escape character
        """
        self.assert_compile(
            column("x").istartswith("a%b_c", escape="\\"),
            "lower(x) LIKE lower(:x_1) || '%' ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_not_istartswith_escape(self):
        self.assert_compile(
            ~column("x").istartswith("a%b_c", escape="\\"),
            "lower(x) NOT LIKE lower(:x_1) || '%' ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_istartswith_autoescape(self):
        """
        apply the escape character to all occurrences of "%", "_" and
        the escape character itself
        """
        self.assert_compile(
            column("x").istartswith("a%b_c/d", autoescape=True),
            "lower(x) LIKE lower(:x_1) || '%' ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_not_istartswith_autoescape(self):
        self.assert_compile(
            ~column("x").istartswith("a%b_c/d", autoescape=True),
            "lower(x) NOT LIKE lower(:x_1) || '%' ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_istartswith_autoescape_custom_escape(self):
        """
        set an escape character to all occurrences of "%", "_" and
        the escape character itself
        """
        self.assert_compile(
            column("x").istartswith("a%b_c/d^e", autoescape=True, escape="^"),
            "lower(x) LIKE lower(:x_1) || '%' ESCAPE '^'",
            checkparams={"x_1": "a^%b^_c/d^^e"},
        )

    def test_istartswith_encoded(self):
        self.assert_compile(
            column("x").istartswith(b"y"),
            "lower(x) LIKE lower(:x_1) || '%'",
            checkparams={"x_1": b"y"},
        )

    def test_not_istartswith_encoded(self):
        self.assert_compile(
            ~column("x").istartswith(b"y"),
            "lower(x) NOT LIKE lower(:x_1) || '%'",
            checkparams={"x_1": b"y"},
        )

    def test_istartswith_encoded_mysql(self):
        self.assert_compile(
            column("x").istartswith(b"y"),
            "lower(x) LIKE concat(lower(%s), '%%')",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_not_istartswith_encoded_mysql(self):
        self.assert_compile(
            ~column("x").istartswith(b"y"),
            "lower(x) NOT LIKE concat(lower(%s), '%%')",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_istartswith_literal(self):
        self.assert_compile(
            column("x").istartswith(literal_column("y")),
            "lower(x) LIKE lower(y) || '%'",
            checkparams={},
        )

    def test_istartswith_text(self):
        self.assert_compile(
            column("x").istartswith(text("y")),
            "lower(x) LIKE lower(y) || '%'",
            checkparams={},
        )

    def test_istartswith_concat(self):
        self.assert_compile(
            column("x").istartswith("y"),
            "lower(x) LIKE concat(lower(%s), '%%')",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_not_istartswith_concat(self):
        self.assert_compile(
            ~column("x").istartswith("y"),
            "lower(x) NOT LIKE concat(lower(%s), '%%')",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_istartswith_literal_mysql(self):
        self.assert_compile(
            column("x").istartswith(literal_column("y")),
            "lower(x) LIKE concat(lower(y), '%%')",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_istartswith_text_mysql(self):
        self.assert_compile(
            column("x").istartswith(text("y")),
            "lower(x) LIKE concat(lower(y), '%%')",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_endswith(self):
        self.assert_compile(
            column("x").endswith("y"),
            "x LIKE '%' || :x_1",
            checkparams={"x_1": "y"},
        )

    def test_endswith_encoded(self):
        self.assert_compile(
            column("x").endswith(b"y"),
            "x LIKE '%' || :x_1",
            checkparams={"x_1": b"y"},
        )

    def test_endswith_encoded_mysql(self):
        self.assert_compile(
            column("x").endswith(b"y"),
            "x LIKE concat('%%', %s)",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_not_endswith_encoded(self):
        self.assert_compile(
            ~column("x").endswith(b"y"),
            "x NOT LIKE '%' || :x_1",
            checkparams={"x_1": b"y"},
        )

    def test_endswith_escape(self):
        self.assert_compile(
            column("x").endswith("a%b_c", escape="\\"),
            "x LIKE '%' || :x_1 ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_endswith_autoescape(self):
        self.assert_compile(
            column("x").endswith("a%b_c/d", autoescape=True),
            "x LIKE '%' || :x_1 ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_endswith_autoescape_custom_escape(self):
        self.assert_compile(
            column("x").endswith("a%b_c/d^e", autoescape=True, escape="^"),
            "x LIKE '%' || :x_1 ESCAPE '^'",
            checkparams={"x_1": "a^%b^_c/d^^e"},
        )

    def test_endswith_autoescape_warning(self):
        with expect_warnings("The autoescape parameter is now a simple"):
            self.assert_compile(
                column("x").endswith("a%b_c/d", autoescape="P"),
                "x LIKE '%' || :x_1 ESCAPE '/'",
                checkparams={"x_1": "a/%b/_c//d"},
            )

    def test_endswith_autoescape_nosqlexpr(self):
        assert_raises_message(
            TypeError,
            "String value expected when autoescape=True",
            column("x").endswith,
            literal_column("'a%b_c/d'"),
            autoescape=True,
        )

    def test_not_endswith(self):
        self.assert_compile(
            ~column("x").endswith("y"),
            "x NOT LIKE '%' || :x_1",
            checkparams={"x_1": "y"},
        )

    def test_not_endswith_pg(self):
        self.assert_compile(
            ~column("x").endswith("y"),
            "x NOT LIKE '%%' || %(x_1)s",
            checkparams={"x_1": "y"},
            dialect="postgresql",
        )

    def test_not_endswith_escape(self):
        self.assert_compile(
            ~column("x").endswith("a%b_c", escape="\\"),
            "x NOT LIKE '%' || :x_1 ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_not_endswith_autoescape(self):
        self.assert_compile(
            ~column("x").endswith("a%b_c/d", autoescape=True),
            "x NOT LIKE '%' || :x_1 ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_endswith_literal(self):
        self.assert_compile(
            column("x").endswith(literal_column("y")),
            "x LIKE '%' || y",
            checkparams={},
        )

    def test_endswith_text(self):
        self.assert_compile(
            column("x").endswith(text("y")), "x LIKE '%' || y", checkparams={}
        )

    def test_endswith_mysql(self):
        self.assert_compile(
            column("x").endswith("y"),
            "x LIKE concat('%%', %s)",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_not_endswith_mysql(self):
        self.assert_compile(
            ~column("x").endswith("y"),
            "x NOT LIKE concat('%%', %s)",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_endswith_literal_mysql(self):
        self.assert_compile(
            column("x").endswith(literal_column("y")),
            "x LIKE concat('%%', y)",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_endswith_text_mysql(self):
        self.assert_compile(
            column("x").endswith(text("y")),
            "x LIKE concat('%%', y)",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_iendswith(self):
        """
        case insensitive endswith method
        """
        self.assert_compile(
            column("x").iendswith("y"),
            "lower(x) LIKE '%' || lower(:x_1)",
            checkparams={"x_1": "y"},
        )

    def test_iendswith_pg(self):
        """
        case insensitive endswith method
        """
        self.assert_compile(
            column("x").iendswith("y"),
            "x ILIKE '%%' || %(x_1)s",
            checkparams={"x_1": "y"},
            dialect="postgresql",
        )

    def test_not_iendswith(self):
        """
        same as test_iendswith but negate the statement
        """
        self.assert_compile(
            ~column("x").iendswith("y"),
            "lower(x) NOT LIKE '%' || lower(:x_1)",
            checkparams={"x_1": "y"},
        )

    def test_not_iendswith_pg(self):
        """
        same as test_iendswith but negate the statement
        """
        self.assert_compile(
            ~column("x").iendswith("y"),
            "x NOT ILIKE '%%' || %(x_1)s",
            checkparams={"x_1": "y"},
            dialect="postgresql",
        )

    def test_iendswith_encoded(self):
        self.assert_compile(
            column("x").iendswith(b"y"),
            "lower(x) LIKE '%' || lower(:x_1)",
            checkparams={"x_1": b"y"},
        )

    def test_not_iendswith_encoded(self):
        self.assert_compile(
            ~column("x").iendswith(b"y"),
            "lower(x) NOT LIKE '%' || lower(:x_1)",
            checkparams={"x_1": b"y"},
        )

    def test_iendswith_encoded_mysql(self):
        self.assert_compile(
            column("x").iendswith(b"y"),
            "lower(x) LIKE concat('%%', lower(%s))",
            checkparams={"x_1": b"y"},
            dialect="mysql",
        )

    def test_iendswith_escape(self):
        """
        render same SQL statement as iendswith but include the ESCAPE
        keyword to establish that character as the escape character
        """
        self.assert_compile(
            column("x").iendswith("a%b_c", escape="\\"),
            "lower(x) LIKE '%' || lower(:x_1) ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_not_iendswith_escape(self):
        self.assert_compile(
            ~column("x").iendswith("a%b_c", escape="\\"),
            "lower(x) NOT LIKE '%' || lower(:x_1) ESCAPE '\\'",
            checkparams={"x_1": "a%b_c"},
        )

    def test_iendswith_autoescape(self):
        """
        apply the escape character to all occurrences of "%", "_" and
        the escape character itself
        """
        self.assert_compile(
            column("x").iendswith("a%b_c/d", autoescape=True),
            "lower(x) LIKE '%' || lower(:x_1) ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_not_iendswith_autoescape(self):
        self.assert_compile(
            ~column("x").iendswith("a%b_c/d", autoescape=True),
            "lower(x) NOT LIKE '%' || lower(:x_1) ESCAPE '/'",
            checkparams={"x_1": "a/%b/_c//d"},
        )

    def test_iendswith_autoescape_custom_escape(self):
        """
        set an escape character to all occurrences of "%", "_" and
        the escape character itself
        """
        self.assert_compile(
            column("x").iendswith("a%b_c/d^e", autoescape=True, escape="^"),
            "lower(x) LIKE '%' || lower(:x_1) ESCAPE '^'",
            checkparams={"x_1": "a^%b^_c/d^^e"},
        )

    def test_iendswith_autoescape_warning(self):
        with expect_warnings("The autoescape parameter is now a simple"):
            self.assert_compile(
                column("x").iendswith("a%b_c/d", autoescape="P"),
                "lower(x) LIKE '%' || lower(:x_1) ESCAPE '/'",
                checkparams={"x_1": "a/%b/_c//d"},
            )

    def test_iendswith_autoescape_nosqlexpr(self):
        assert_raises_message(
            TypeError,
            "String value expected when autoescape=True",
            column("x").iendswith,
            literal_column("'a%b_c/d'"),
            autoescape=True,
        )

    def test_iendswith_literal(self):
        self.assert_compile(
            column("x").iendswith(literal_column("y")),
            "lower(x) LIKE '%' || lower(y)",
            checkparams={},
        )

    def test_iendswith_text(self):
        self.assert_compile(
            column("x").iendswith(text("y")),
            "lower(x) LIKE '%' || lower(y)",
            checkparams={},
        )

    def test_iendswith_mysql(self):
        self.assert_compile(
            column("x").iendswith("y"),
            "lower(x) LIKE concat('%%', lower(%s))",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_not_iendswith_mysql(self):
        self.assert_compile(
            ~column("x").iendswith("y"),
            "lower(x) NOT LIKE concat('%%', lower(%s))",
            checkparams={"x_1": "y"},
            dialect=mysql.dialect(),
        )

    def test_iendswith_literal_mysql(self):
        self.assert_compile(
            column("x").iendswith(literal_column("y")),
            "lower(x) LIKE concat('%%', lower(y))",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_iendswith_text_mysql(self):
        self.assert_compile(
            column("x").iendswith(text("y")),
            "lower(x) LIKE concat('%%', lower(y))",
            checkparams={},
            dialect=mysql.dialect(),
        )


class CustomOpTest(fixtures.TestBase):
    def test_is_comparison_legacy(self):
        c = column("x")
        c2 = column("y")
        op1 = c.op("$", is_comparison=True)(c2).operator
        op2 = c.op("$", is_comparison=False)(c2).operator

        assert operators.is_comparison(op1)
        assert not operators.is_comparison(op2)

    def test_is_comparison_bool_op(self):
        c = column("x")
        c2 = column("y")
        op1 = c.bool_op("$")(c2).operator
        op2 = c.op("$")(c2).operator

        assert operators.is_comparison(op1)
        assert not operators.is_comparison(op2)

    @testing.combinations(
        (sqltypes.NULLTYPE,),
        (Integer(),),
        (ARRAY(String),),
        (String(50),),
        (Boolean(),),
        (DateTime(),),
        (sqltypes.JSON(),),
        (postgresql.ARRAY(Integer),),
        (sqltypes.Numeric(5, 2),),
        id_="r",
    )
    def test_return_types(self, typ):
        some_return_type = sqltypes.DECIMAL()

        c = column("x", typ)
        expr = c.op("$", is_comparison=True)(None)
        is_(expr.type, sqltypes.BOOLEANTYPE)

        c = column("x", typ)
        expr = c.bool_op("$")(None)
        is_(expr.type, sqltypes.BOOLEANTYPE)

        expr = c.op("$")(None)
        is_(expr.type, typ)

        expr = c.op("$", return_type=some_return_type)(None)
        is_(expr.type, some_return_type)

        expr = c.op("$", is_comparison=True, return_type=some_return_type)(
            None
        )
        is_(expr.type, some_return_type)

    def test_python_impl(self):
        """test #3162"""
        c = column("x")
        c2 = column("y")
        op1 = c.op("$", python_impl=lambda a, b: a > b)(c2).operator

        is_(op1(3, 5), False)
        is_(op1(5, 3), True)

    def test_python_impl_not_present(self):
        """test #3162"""
        c = column("x")
        c2 = column("y")
        op1 = c.op("$")(c2).operator

        with expect_raises_message(
            exc.InvalidRequestError,
            r"Custom operator '\$' can't be used with plain Python objects "
            "unless it includes the 'python_impl' parameter.",
        ):
            op1(3, 5)

    def test_operator_class_default(self):
        """Test that custom_op defaults to OperatorClass.BASE"""
        op = operators.custom_op("++")
        eq_(op.operator_class, OperatorClass.BASE)

    def test_operator_class_explicit(self):
        """Test that custom_op accepts an explicit operator_class parameter"""
        op = operators.custom_op("++", operator_class=OperatorClass.MATH)
        eq_(op.operator_class, OperatorClass.MATH)

    def test_operator_class_combined(self):
        """Test that custom_op accepts combined operator classes"""
        op = operators.custom_op(
            "++", operator_class=OperatorClass.MATH | OperatorClass.BITWISE
        )
        eq_(op.operator_class, OperatorClass.MATH | OperatorClass.BITWISE)

    def test_operator_class_with_column_op(self):
        """Test that operator_class is passed through when using column.op()"""
        c = column("x", Integer)

        expr1 = c.op("++")("value")
        eq_(expr1.operator.operator_class, OperatorClass.BASE)

        expr2 = c.op("++", operator_class=OperatorClass.MATH)("value")
        eq_(expr2.operator.operator_class, OperatorClass.MATH)

        with expect_deprecated(
            r"Type object .*Integer.* does not include custom "
            r"operator '\+\+' in its operator classes."
        ):
            expr3 = c.op("++", operator_class=OperatorClass.STRING_MATCH)(
                "value"
            )
        eq_(expr3.operator.operator_class, OperatorClass.STRING_MATCH)

    def test_operator_class_hash_and_equality(self):
        op1 = operators.custom_op("++", operator_class=OperatorClass.MATH)
        op2 = operators.custom_op("++", operator_class=OperatorClass.MATH)
        op3 = operators.custom_op("++", operator_class=OperatorClass.BITWISE)

        # Same opstring and same operator_class should be equal
        eq_(op1, op2)
        eq_(hash(op1), hash(op2))

        # Same opstring but different operator_class should be different
        ne_(op1, op3)
        ne_(hash(op1), hash(op3))

    def test_operator_class_warning_unspecified_type(self):
        """Test warning when type has UNSPECIFIED operator_classes"""

        # Create a custom type with UNSPECIFIED operator_classes
        class UnspecifiedType(TypeEngine):
            operator_classes = OperatorClass.UNSPECIFIED

        metadata = MetaData()
        test_table = Table(
            "test", metadata, Column("value", UnspecifiedType())
        )
        col = test_table.c.value

        # Use a builtin operator that should not be compatible
        # This should trigger the first deprecation warning
        with expect_deprecated(
            "Type object .* does not refer to an OperatorClass in "
            "its operator_classes attribute"
        ):
            col == "test"


class TupleTypingTest(fixtures.TestBase):
    def _assert_types(self, expr):
        eq_(expr[0]._type_affinity, Integer)
        eq_(expr[1]._type_affinity, String)
        eq_(expr[2]._type_affinity, LargeBinary()._type_affinity)

    def test_type_coercion_on_eq(self):
        a, b, c = (
            column("a", Integer),
            column("b", String),
            column("c", LargeBinary),
        )
        t1 = tuple_(a, b, c)
        expr = t1 == (3, "hi", "there")
        self._assert_types([bind.type for bind in expr.right.clauses])

    def test_type_coercion_on_in(self):
        a, b, c = (
            column("a", Integer),
            column("b", String),
            column("c", LargeBinary),
        )
        t1 = tuple_(a, b, c)
        expr = t1.in_([(3, "hi", "there"), (4, "Q", "P")])

        eq_(len(expr.right.value), 2)

        self._assert_types(expr.right.type.types)

    # since we want to infer "binary"
    def test_tuple_type_expanding_inference(self):
        a, b, c = column("a"), column("b"), column("c")

        t1 = tuple_(a, b, c)
        expr = t1.in_([(3, "hi", b"there"), (4, "Q", b"P")])

        eq_(len(expr.right.value), 2)

        self._assert_types(expr.right.type.types)

    def test_tuple_type_plain_inference(self):
        a, b, c = column("a"), column("b"), column("c")

        t1 = tuple_(a, b, c)
        expr = t1 == (3, "hi", b"there")
        self._assert_types(expr.right.type.types)

    def test_tuple_type_left_type_ignored(self):
        a, b = column("a", sqltypes.Date), column("b", sqltypes.DateTime)
        c = column("c", sqltypes.Float)

        t1 = tuple_(a, b, c)
        expr = t1.in_([(3, "hi", b"there")])
        self._assert_types(expr.right.type.types)


class InSelectableTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    def test_in_select(self):
        t = table("t", column("x"))

        stmt = select(t.c.x)

        self.assert_compile(column("q").in_(stmt), "q IN (SELECT t.x FROM t)")

    def test_in_subquery_warning(self):
        t = table("t", column("x"))

        stmt = select(t.c.x).subquery()

        with expect_warnings(
            r"Coercing Subquery object into a select\(\) for use in "
            r"IN\(\); please pass a select\(\) construct explicitly",
        ):
            self.assert_compile(
                column("q").in_(stmt),
                "q IN (SELECT anon_1.x FROM "
                "(SELECT t.x AS x FROM t) AS anon_1)",
            )

    def test_in_subquery_explicit(self):
        t = table("t", column("x"))

        stmt = select(t.c.x).subquery()

        self.assert_compile(
            column("q").in_(stmt.select()),
            "q IN (SELECT anon_1.x FROM "
            "(SELECT t.x AS x FROM t) AS anon_1)",
        )

    def test_in_subquery_alias_implicit(self):
        t = table("t", column("x"))

        stmt = select(t.c.x).subquery().alias()

        with expect_warnings(
            r"Coercing Alias object into a select\(\) for use in "
            r"IN\(\); please pass a select\(\) construct explicitly",
        ):
            self.assert_compile(
                column("q").in_(stmt),
                "q IN (SELECT anon_1.x FROM (SELECT t.x AS x FROM t) "
                "AS anon_1)",
            )

    def test_in_subquery_alias_explicit(self):
        t = table("t", column("x"))

        stmt = select(t.c.x).subquery().alias()

        self.assert_compile(
            column("q").in_(stmt.select().scalar_subquery()),
            "q IN (SELECT anon_1.x FROM (SELECT t.x AS x FROM t) AS anon_1)",
        )

    def test_in_table(self):
        t = table("t", column("x"))

        with expect_warnings(
            r"Coercing TableClause object into a select\(\) for use in "
            r"IN\(\); please pass a select\(\) construct explicitly",
        ):
            self.assert_compile(column("q").in_(t), "q IN (SELECT t.x FROM t)")

    def test_in_table_alias(self):
        t = table("t", column("x"))

        with expect_warnings(
            r"Coercing Alias object into a select\(\) for use in "
            r"IN\(\); please pass a select\(\) construct explicitly",
        ):
            self.assert_compile(
                column("q").in_(t.alias()), "q IN (SELECT t_1.x FROM t AS t_1)"
            )

    def test_in_cte_implicit(self):
        t = table("t", column("x"))

        stmt = select(t.c.x).cte()

        with expect_warnings(
            r"Coercing CTE object into a select\(\) for use in "
            r"IN\(\); please pass a select\(\) construct explicitly",
        ):
            s2 = select(column("q").in_(stmt))

        self.assert_compile(
            s2,
            "WITH anon_2 AS (SELECT t.x AS x FROM t) "
            "SELECT q IN (SELECT anon_2.x FROM anon_2) AS anon_1",
        )

    def test_in_cte_explicit(self):
        t = table("t", column("x"))

        stmt = select(t.c.x).cte()

        s2 = select(column("q").in_(stmt.select().scalar_subquery()))

        self.assert_compile(
            s2,
            "WITH anon_2 AS (SELECT t.x AS x FROM t) "
            "SELECT q IN (SELECT anon_2.x FROM anon_2) AS anon_1",
        )

    def test_in_cte_select(self):
        t = table("t", column("x"))

        stmt = select(t.c.x).cte()

        s2 = select(column("q").in_(stmt.select()))

        self.assert_compile(
            s2,
            "WITH anon_2 AS (SELECT t.x AS x FROM t) "
            "SELECT q IN (SELECT anon_2.x FROM anon_2) AS anon_1",
        )


class MyEnum(enum.Enum):
    ONE = enum.auto()
    TWO = enum.auto()
    THREE = enum.auto()


class AnyAllTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def t_fixture(self):
        m = MetaData()

        t = Table(
            "tab1",
            m,
            Column("arrval", ARRAY(Integer)),
            Column("arrenum", ARRAY(Enum(MyEnum))),
            Column("arrstring", ARRAY(String)),
            Column("data", Integer),
        )
        return t

    null_comparisons = testing.combinations(
        lambda col: any_(col) == None,
        lambda col: col.any_() == None,
        lambda col: any_(col) == null(),
        lambda col: col.any_() == null(),
        lambda col: null() == any_(col),
        lambda col: null() == col.any_(),
        lambda col: None == any_(col),
        lambda col: None == col.any_(),
        argnames="expr",
    )

    @null_comparisons
    @testing.combinations("int", "array", argnames="datatype")
    def test_any_generic_null(self, datatype, expr, t_fixture):
        col = t_fixture.c.data if datatype == "int" else t_fixture.c.arrval

        self.assert_compile(expr(col), "NULL = ANY (tab1.%s)" % col.name)

    @null_comparisons
    @testing.combinations("int", "array", argnames="datatype")
    def test_any_generic_null_negate(self, datatype, expr, t_fixture):
        col = t_fixture.c.data if datatype == "int" else t_fixture.c.arrval

        self.assert_compile(
            ~expr(col), "NOT (NULL = ANY (tab1.%s))" % col.name
        )

    @testing.variation("operator", ["any", "all"])
    @testing.variation(
        "datatype", ["int", "array", "arraystring", "arrayenum"]
    )
    def test_what_type_is_any_all(
        self,
        datatype: testing.Variation,
        t_fixture,
        operator: testing.Variation,
    ):
        """test for #12874"""

        if datatype.int:
            col = t_fixture.c.data
            value = 5
            expected_type_affinity = Integer
        elif datatype.array:
            col = t_fixture.c.arrval
            value = 25
            expected_type_affinity = Integer
        elif datatype.arraystring:
            col = t_fixture.c.arrstring
            value = "a string"
            expected_type_affinity = String
        elif datatype.arrayenum:
            col = t_fixture.c.arrenum
            value = MyEnum.TWO
            expected_type_affinity = Enum
        else:
            datatype.fail()

        if operator.any:
            boolean_expr = value == any_(col)
        elif operator.all:
            boolean_expr = value == all_(col)
        else:
            operator.fail()

        # using isinstance so things work out for Enum which has type affinity
        # of String
        assert isinstance(boolean_expr.left.type, expected_type_affinity)

    @testing.fixture(
        params=[
            ("ANY", any_),
            ("ANY", lambda x: x.any_()),
            ("ALL", all_),
            ("ALL", lambda x: x.all_()),
        ]
    )
    def any_all_operators(self, request):
        return request.param

    def test_array(self, t_fixture, any_all_operators):
        t = t_fixture
        op, fn = any_all_operators
        self.assert_compile(
            5 == fn(t.c.arrval),
            f":param_1 = {op} (tab1.arrval)",
            checkparams={"param_1": 5},
        )

    def test_comparator_inline_negate(self, t_fixture, any_all_operators):
        t = t_fixture
        op, fn = any_all_operators
        self.assert_compile(
            5 != fn(t.c.arrval),
            f":param_1 != {op} (tab1.arrval)",
            checkparams={"param_1": 5},
        )

    @testing.combinations(
        (operator.eq, "="),
        (operator.ne, "!="),
        (operator.gt, ">"),
        (operator.le, "<="),
        argnames="operator,opstring",
    )
    def test_comparator_outer_negate(
        self, t_fixture, any_all_operators, operator, opstring
    ):
        """test #10817"""
        t = t_fixture
        op, fn = any_all_operators
        self.assert_compile(
            ~(operator(5, fn(t.c.arrval))),
            f"NOT (:param_1 {opstring} {op} (tab1.arrval))",
            checkparams={"param_1": 5},
        )

    def test_comparator_array(self, t_fixture, any_all_operators):
        t = t_fixture
        op, fn = any_all_operators
        self.assert_compile(
            5 > fn(t.c.arrval),
            f":param_1 > {op} (tab1.arrval)",
            checkparams={"param_1": 5},
        )

    def test_comparator_array_wexpr(self, t_fixture, any_all_operators):
        t = t_fixture
        op, fn = any_all_operators
        self.assert_compile(
            t.c.data > fn(t.c.arrval),
            f"tab1.data > {op} (tab1.arrval)",
            checkparams={},
        )

    def test_illegal_ops(self, t_fixture, any_all_operators):
        t = t_fixture
        op, fn = any_all_operators

        assert_raises_message(
            exc.ArgumentError,
            "Only comparison operators may be used with ANY/ALL",
            lambda: 5 + fn(t.c.arrval),
        )

        # TODO:
        # this is invalid but doesn't raise an error,
        # as the left-hand side just does its thing.  Types
        # would need to reject their right-hand side.
        self.assert_compile(
            t.c.data + fn(t.c.arrval), f"tab1.data + {op} (tab1.arrval)"
        )

    def test_array_expression(self, t_fixture, any_all_operators):
        t = t_fixture
        op, fn = any_all_operators

        self.assert_compile(
            5 == fn(t.c.arrval[5:6] + postgresql.array([3, 4])),
            f"%(param_1)s = {op} (tab1.arrval[%(arrval_1)s:%(arrval_2)s] || "
            "ARRAY[%(param_2)s, %(param_3)s])",
            checkparams={
                "arrval_2": 6,
                "param_1": 5,
                "param_3": 4,
                "arrval_1": 5,
                "param_2": 3,
            },
            dialect="postgresql",
        )

    def test_subq(self, t_fixture, any_all_operators):
        t = t_fixture
        op, fn = any_all_operators

        self.assert_compile(
            5 == fn(select(t.c.data).where(t.c.data < 10).scalar_subquery()),
            f":param_1 = {op} (SELECT tab1.data "
            "FROM tab1 WHERE tab1.data < :data_1)",
            checkparams={"data_1": 10, "param_1": 5},
        )

    def test_scalar_values(self, t_fixture, any_all_operators):
        t = t_fixture
        op, fn = any_all_operators

        self.assert_compile(
            5 == fn(values(t.c.data).data([(1,), (42,)]).scalar_values()),
            f":param_1 = {op} (VALUES (:param_2), (:param_3))",
            checkparams={"param_1": 5, "param_2": 1, "param_3": 42},
        )

    @testing.combinations(any_, all_, argnames="fn")
    def test_values_illegal(self, t_fixture, fn):
        t = t_fixture

        with expect_raises_message(
            exc.ArgumentError,
            "SQL expression element expected, got .* "
            "To create a column expression from a VALUES clause, "
            r"use the .scalar_values\(\) method.",
        ):
            fn(values(t.c.data).data([(1,), (42,)]))


class DeprecatedAnyAllTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.fixture
    def t_fixture(self):
        m = MetaData()

        t = Table(
            "tab1",
            m,
            Column("arrval", ARRAY(Integer)),
            Column("arrenum", ARRAY(Enum(MyEnum))),
            Column("arrstring", ARRAY(String)),
            Column("data", Integer),
        )
        return t

    # test legacy array any() / all().  these are superseded by the
    # any_() / all_() versions
    @testing.fixture(
        params=[
            ("ANY", lambda x, *o: x.any(*o)),
            ("ALL", lambda x, *o: x.all(*o)),
        ]
    )
    def legacy_any_all_operators(self, request):
        return request.param

    def _array_any_deprecation(self):
        return testing.expect_deprecated(
            r"The ARRAY.Comparator.any\(\) and "
            r"ARRAY.Comparator.all\(\) methods "
            r"for arrays are deprecated for removal, along with the "
            r"PG-specific Any\(\) "
            r"and All\(\) functions. See any_\(\) and all_\(\) functions for "
            "modern use. "
        )

    @testing.variation("operator", ["any", "all"])
    @testing.variation("datatype", ["array", "arraystring", "arrayenum"])
    def test_what_type_is_legacy_any_all(
        self,
        datatype: testing.Variation,
        t_fixture,
        operator: testing.Variation,
    ):
        if datatype.array:
            col = t_fixture.c.arrval
            value = 25
            expected_type_affinity = Integer
        elif datatype.arraystring:
            col = t_fixture.c.arrstring
            value = "a string"
            expected_type_affinity = String
        elif datatype.arrayenum:
            col = t_fixture.c.arrenum
            value = MyEnum.TWO
            expected_type_affinity = Enum
        else:
            datatype.fail()

        with self._array_any_deprecation():
            if operator.any:
                boolean_expr = col.any(value)
            elif operator.all:
                boolean_expr = col.all(value)
            else:
                operator.fail()

        # using isinstance so things work out for Enum which has type affinity
        # of String
        assert isinstance(boolean_expr.left.type, expected_type_affinity)

    def test_bindparam_coercion(self, t_fixture, legacy_any_all_operators):
        """test #7979"""
        t = t_fixture
        op, fn = legacy_any_all_operators

        with self._array_any_deprecation():
            expr = fn(t.c.arrval, bindparam("param"))
        expected = f"%(param)s = {op} (tab1.arrval)"
        is_(expr.left.type._type_affinity, Integer)

        self.assert_compile(expr, expected, dialect="postgresql")

    def test_array_comparator_accessor(
        self, t_fixture, legacy_any_all_operators
    ):
        t = t_fixture
        op, fn = legacy_any_all_operators

        with self._array_any_deprecation():
            self.assert_compile(
                fn(t.c.arrval, 5, operator.gt),
                f":arrval_1 > {op} (tab1.arrval)",
                checkparams={"arrval_1": 5},
            )

    def test_array_comparator_negate_accessor(
        self, t_fixture, legacy_any_all_operators
    ):
        t = t_fixture
        op, fn = legacy_any_all_operators

        with self._array_any_deprecation():
            self.assert_compile(
                ~fn(t.c.arrval, 5, operator.gt),
                f"NOT (:arrval_1 > {op} (tab1.arrval))",
                checkparams={"arrval_1": 5},
            )


class BitOpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = "default"

    @testing.combinations(
        ("neg", operators.neg, "-"),
        ("inv", operators.inv, "NOT "),
        ("not", operators.bitwise_not_op, "~"),
        ("distinct", operators.distinct_op, "DISTINCT "),
        id_="iaa",
        argnames="py_op, sql_op",
    )
    @testing.variation("named", ["column", "unnamed", "label"])
    def test_wraps_named_column_heuristic(self, py_op, sql_op, named):
        """test for #12681"""

        if named.column:
            expr = py_op(column("q", Integer))
            assert isinstance(expr, UnaryExpression)

            self.assert_compile(
                select(expr),
                f"SELECT {sql_op}q",
            )

        elif named.unnamed:
            expr = py_op(literal("x", Integer))
            assert isinstance(expr, UnaryExpression)

            self.assert_compile(
                select(expr),
                f"SELECT {sql_op}:param_1 AS anon_1",
            )
        elif named.label:
            expr = py_op(literal("x", Integer).label("z"))
            if py_op is operators.inv:
                # special case for operators.inv due to Label._negate()
                # not sure if this should be changed but still works out in the
                # end
                assert isinstance(expr.element, UnaryExpression)
            else:
                assert isinstance(expr, UnaryExpression)

            self.assert_compile(
                select(expr),
                f"SELECT {sql_op}:param_1 AS z",
            )

    def test_compile_not_column_lvl(self):
        c = column("c", Integer)

        self.assert_compile(
            select(c.bitwise_not()),
            "SELECT ~c",
        )

    def test_compile_not_colexpr_lvl(self):
        c = column("c", Integer)

        self.assert_compile(
            select(operators.bitwise_not_op(c)),
            "SELECT ~c",
        )

    @testing.combinations(
        ("xor", operators.bitwise_xor_op, "^"),
        ("xor_lambda", lambda c1, c2: c1.bitwise_xor(c2), "^"),
        ("or", operators.bitwise_or_op, "|"),
        ("or_lambda", lambda c1, c2: c1.bitwise_or(c2), "|"),
        ("and", operators.bitwise_and_op, "&"),
        ("and_lambda", lambda c1, c2: c1.bitwise_and(c2), "&"),
        ("lshift", operators.bitwise_lshift_op, "<<"),
        ("ls_lambda", lambda c1, c2: c1.bitwise_lshift(c2), "<<"),
        ("rshift", operators.bitwise_rshift_op, ">>"),
        ("rs_lambda", lambda c1, c2: c1.bitwise_rshift(c2), ">>"),
        id_="iaa",
    )
    def test_compile_binary(self, py_op, sql_op):
        c1 = column("c1", Integer)
        c2 = column("c2", Integer)

        self.assert_compile(
            select(py_op(c1, c2)),
            f"SELECT c1 {sql_op} c2 AS anon_1",
        )


class StandaloneOperatorTranslateTest(
    fixtures.TestBase, testing.AssertsCompiledSQL
):
    __dialect__ = "default"

    def _combinations(fn):
        return testing.combinations(
            desc,
            asc,
            nulls_first,
            nulls_last,
            any_,
            all_,
            distinct,
            bitwise_not,
            collate,
        )(fn)

    @_combinations
    def test_move(self, operator):
        m1 = column("q")
        m2 = mock.Mock()

        class MyCustomThing(roles.ByOfRole, SQLColumnExpression):
            def __clause_element__(self):
                return m1

            @property
            def comparator(self):
                return Comparator()

            def operate(
                self,
                op,
                *other,
                **kwargs,
            ):
                return op(self.comparator, *other, **kwargs)

            def reverse_operate(
                self,
                op,
                *other,
                **kwargs,
            ):
                return op(other, self.comparator, **kwargs)

        class Comparator(ColumnOperators):
            def _operate(self, *arg, **kw):
                return m2

        setattr(Comparator, operator.__name__, Comparator._operate)

        mc = MyCustomThing()

        if operator is collate:
            result = operator(mc, "some collation")
        else:
            result = operator(mc)

        is_(result, m2)

    @_combinations
    def test_text(self, operator):
        if operator is collate:
            result = operator(text("foo"), "some collation")
        else:
            result = operator(text("foo"))

        # Assert that the operation completed without crashing
        # and returned a valid SQL expression
        assert result is not None
