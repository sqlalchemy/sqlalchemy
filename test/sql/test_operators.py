from test.lib import fixtures, testing
from test.lib.testing import assert_raises_message
from sqlalchemy.sql import column, desc, asc, literal, collate
from sqlalchemy.sql.expression import BinaryExpression, \
                ClauseList, Grouping, _DefaultColumnComparator,\
                UnaryExpression
from sqlalchemy.sql import operators
from sqlalchemy import exc
from sqlalchemy.schema import Column, Table, MetaData
from sqlalchemy.types import Integer, TypeEngine, TypeDecorator

class DefaultColumnComparatorTest(fixtures.TestBase):

    def _do_scalar_test(self, operator, compare_to):
        left = column('left')
        assert left.comparator.operate(operator).compare(
            compare_to(left)
        )

    def _do_operate_test(self, operator):
        left = column('left')
        right = column('right')

        assert left.comparator.operate(operator, right).compare(
            BinaryExpression(left, right, operator)
        )

    def test_desc(self):
        self._do_scalar_test(operators.desc_op, desc)

    def test_asc(self):
        self._do_scalar_test(operators.asc_op, asc)

    def test_plus(self):
        self._do_operate_test(operators.add)

    def test_no_getitem(self):
        assert_raises_message(
            NotImplementedError,
            "Operator 'getitem' is not supported on this expression",
            self._do_operate_test, operators.getitem
        )
        assert_raises_message(
            NotImplementedError,
            "Operator 'getitem' is not supported on this expression",
            lambda: column('left')[3]
        )

    def test_in(self):
        left = column('left')
        assert left.comparator.operate(operators.in_op, [1, 2, 3]).compare(
                BinaryExpression(
                    left,
                    Grouping(ClauseList(
                        literal(1), literal(2), literal(3)
                    )),
                    operators.in_op
                )
            )

    def test_collate(self):
        left = column('left')
        right = "some collation"
        left.comparator.operate(operators.collate, right).compare(
            collate(left, right)
        )

    def test_concat(self):
        self._do_operate_test(operators.concat_op)

class CustomUnaryOperatorTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    def _factorial_fixture(self):
        class MyInteger(Integer):
            class comparator_factory(Integer.Comparator):
                def factorial(self):
                    return UnaryExpression(self.expr,
                                modifier=operators.custom_op("!"),
                                type_=MyInteger)

                def factorial_prefix(self):
                    return UnaryExpression(self.expr,
                                operator=operators.custom_op("!!"),
                                type_=MyInteger)

        return MyInteger

    def test_factorial(self):
        col = column('somecol', self._factorial_fixture())
        self.assert_compile(
            col.factorial(),
            "somecol !"
        )

    def test_double_factorial(self):
        col = column('somecol', self._factorial_fixture())
        self.assert_compile(
            col.factorial().factorial(),
            "somecol ! !"
        )

    def test_factorial_prefix(self):
        col = column('somecol', self._factorial_fixture())
        self.assert_compile(
            col.factorial_prefix(),
            "!! somecol"
        )

    def test_unary_no_ops(self):
        assert_raises_message(
            exc.CompileError,
            "Unary expression has no operator or modifier",
            UnaryExpression(literal("x")).compile
        )

    def test_unary_both_ops(self):
        assert_raises_message(
            exc.CompileError,
            "Unary expression does not support operator and "
                "modifier simultaneously",
            UnaryExpression(literal("x"),
                    operator=operators.custom_op("x"),
                    modifier=operators.custom_op("y")).compile
        )

class _CustomComparatorTests(object):
    def test_override_builtin(self):
        c1 = Column('foo', self._add_override_factory())
        self._assert_add_override(c1)

    def test_column_proxy(self):
        t = Table('t', MetaData(),
                Column('foo', self._add_override_factory())
            )
        proxied = t.select().c.foo
        self._assert_add_override(proxied)

    def test_alias_proxy(self):
        t = Table('t', MetaData(),
                Column('foo', self._add_override_factory())
            )
        proxied = t.alias().c.foo
        self._assert_add_override(proxied)

    def test_binary_propagate(self):
        c1 = Column('foo', self._add_override_factory())
        self._assert_add_override(c1 - 6)

    def test_reverse_binary_propagate(self):
        c1 = Column('foo', self._add_override_factory())
        self._assert_add_override(6 - c1)

    def test_binary_multi_propagate(self):
        c1 = Column('foo', self._add_override_factory())
        self._assert_add_override((c1 - 6) + 5)

    def test_no_boolean_propagate(self):
        c1 = Column('foo', self._add_override_factory())
        self._assert_not_add_override(c1 == 56)

    def _assert_add_override(self, expr):
        assert (expr + 5).compare(
            expr.op("goofy")(5)
        )

    def _assert_not_add_override(self, expr):
        assert not (expr + 5).compare(
            expr.op("goofy")(5)
        )

class CustomComparatorTest(_CustomComparatorTests, fixtures.TestBase):
    def _add_override_factory(self):

        class MyInteger(Integer):
            class comparator_factory(TypeEngine.Comparator):
                def __init__(self, expr):
                    self.expr = expr

                def __add__(self, other):
                    return self.expr.op("goofy")(other)


        return MyInteger


class TypeDecoratorComparatorTest(_CustomComparatorTests, fixtures.TestBase):
    def _add_override_factory(self):

        class MyInteger(TypeDecorator):
            impl = Integer

            class comparator_factory(TypeEngine.Comparator):
                def __init__(self, expr):
                    self.expr = expr

                def __add__(self, other):
                    return self.expr.op("goofy")(other)


        return MyInteger


class CustomEmbeddedinTypeDecoratorTest(_CustomComparatorTests, fixtures.TestBase):
    def _add_override_factory(self):
        class MyInteger(Integer):
            class comparator_factory(TypeEngine.Comparator):
                def __init__(self, expr):
                    self.expr = expr

                def __add__(self, other):
                    return self.expr.op("goofy")(other)


        class MyDecInteger(TypeDecorator):
            impl = MyInteger

        return MyDecInteger

class NewOperatorTest(_CustomComparatorTests, fixtures.TestBase):
    def _add_override_factory(self):
        class MyInteger(Integer):
            class comparator_factory(TypeEngine.Comparator):
                def __init__(self, expr):
                    self.expr = expr

                def foob(self, other):
                    return self.expr.op("foob")(other)
        return MyInteger

    def _assert_add_override(self, expr):
        assert (expr.foob(5)).compare(
            expr.op("foob")(5)
        )

    def _assert_not_add_override(self, expr):
        assert not hasattr(expr, "foob")

from sqlalchemy import and_, not_, between

class OperatorPrecedenceTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_operator_precedence(self):
        # TODO: clean up /break up
        metadata = MetaData()
        table = Table('op', metadata,
            Column('field', Integer))
        self.assert_compile(table.select((table.c.field == 5) == None),
            "SELECT op.field FROM op WHERE (op.field = :field_1) IS NULL")
        self.assert_compile(table.select((table.c.field + 5) == table.c.field),
            "SELECT op.field FROM op WHERE op.field + :field_1 = op.field")
        self.assert_compile(table.select((table.c.field + 5) * 6),
            "SELECT op.field FROM op WHERE (op.field + :field_1) * :param_1")
        self.assert_compile(table.select((table.c.field * 5) + 6),
            "SELECT op.field FROM op WHERE op.field * :field_1 + :param_1")
        self.assert_compile(table.select(5 + table.c.field.in_([5, 6])),
            "SELECT op.field FROM op WHERE :param_1 + "
                        "(op.field IN (:field_1, :field_2))")
        self.assert_compile(table.select((5 + table.c.field).in_([5, 6])),
            "SELECT op.field FROM op WHERE :field_1 + op.field "
                    "IN (:param_1, :param_2)")
        self.assert_compile(table.select(not_(and_(table.c.field == 5,
                        table.c.field == 7))),
            "SELECT op.field FROM op WHERE NOT "
                "(op.field = :field_1 AND op.field = :field_2)")
        self.assert_compile(table.select(not_(table.c.field == 5)),
            "SELECT op.field FROM op WHERE op.field != :field_1")
        self.assert_compile(table.select(not_(table.c.field.between(5, 6))),
            "SELECT op.field FROM op WHERE NOT "
                    "(op.field BETWEEN :field_1 AND :field_2)")
        self.assert_compile(table.select(not_(table.c.field) == 5),
            "SELECT op.field FROM op WHERE (NOT op.field) = :param_1")
        self.assert_compile(table.select((table.c.field == table.c.field).\
                            between(False, True)),
            "SELECT op.field FROM op WHERE (op.field = op.field) "
                            "BETWEEN :param_1 AND :param_2")
        self.assert_compile(table.select(
                        between((table.c.field == table.c.field), False, True)),
            "SELECT op.field FROM op WHERE (op.field = op.field) "
                    "BETWEEN :param_1 AND :param_2")

class OperatorAssociativityTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_associativity(self):
        # TODO: clean up /break up
        f = column('f')
        self.assert_compile(f - f, "f - f")
        self.assert_compile(f - f - f, "(f - f) - f")

        self.assert_compile((f - f) - f, "(f - f) - f")
        self.assert_compile((f - f).label('foo') - f, "(f - f) - f")

        self.assert_compile(f - (f - f), "f - (f - f)")
        self.assert_compile(f - (f - f).label('foo'), "f - (f - f)")

        # because - less precedent than /
        self.assert_compile(f / (f - f), "f / (f - f)")
        self.assert_compile(f / (f - f).label('foo'), "f / (f - f)")

        self.assert_compile(f / f - f, "f / f - f")
        self.assert_compile((f / f) - f, "f / f - f")
        self.assert_compile((f / f).label('foo') - f, "f / f - f")

        # because / more precedent than -
        self.assert_compile(f - (f / f), "f - f / f")
        self.assert_compile(f - (f / f).label('foo'), "f - f / f")
        self.assert_compile(f - f / f, "f - f / f")
        self.assert_compile((f - f) / f, "(f - f) / f")

        self.assert_compile(((f - f) / f) - f, "(f - f) / f - f")
        self.assert_compile((f - f) / (f - f), "(f - f) / (f - f)")

        # higher precedence
        self.assert_compile((f / f) - (f / f), "f / f - f / f")

        self.assert_compile((f / f) - (f - f), "f / f - (f - f)")
        self.assert_compile((f / f) / (f - f), "(f / f) / (f - f)")
        self.assert_compile(f / (f / (f - f)), "f / (f / (f - f))")


