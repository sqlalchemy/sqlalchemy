from test.lib import fixtures
from sqlalchemy.sql import column, desc, asc, literal, collate
from sqlalchemy.sql.expression import BinaryExpression, \
                ClauseList, Grouping, _DefaultColumnComparator
from sqlalchemy.sql import operators
from sqlalchemy.schema import Column, Table, MetaData
from sqlalchemy.types import Integer

class DefaultColumnComparatorTest(fixtures.TestBase):

    def _do_scalar_test(self, operator, compare_to):
        cc = _DefaultColumnComparator()
        left = column('left')
        assert cc.operate(left, operator).compare(
            compare_to(left)
        )

    def _do_operate_test(self, operator):
        cc = _DefaultColumnComparator()
        left = column('left')
        right = column('right')

        assert cc.operate(left, operator, right).compare(
            BinaryExpression(left, right, operator)
        )

    def test_desc(self):
        self._do_scalar_test(operators.desc_op, desc)

    def test_asc(self):
        self._do_scalar_test(operators.asc_op, asc)

    def test_plus(self):
        self._do_operate_test(operators.add)

    def test_in(self):
        cc = _DefaultColumnComparator()
        left = column('left')
        assert cc.operate(left, operators.in_op, [1, 2, 3]).compare(
                BinaryExpression(
                    left,
                    Grouping(ClauseList(
                        literal(1), literal(2), literal(3)
                    )),
                    operators.in_op
                )
            )

    def test_collate(self):
        cc = _DefaultColumnComparator()
        left = column('left')
        right = "some collation"
        cc.operate(left, operators.collate, right).compare(
            collate(left, right)
        )

class CustomComparatorTest(fixtures.TestBase):
    def _add_override_factory(self):
        class MyComparator(Column.Comparator):
            def __init__(self, expr):
                self.expr = expr

            def __add__(self, other):
                return self.expr.op("goofy")(other)
        return MyComparator

    def _assert_add_override(self, expr):
        assert (expr + 5).compare(
            expr.op("goofy")(5)
        )

    def _assert_not_add_override(self, expr):
        assert not (expr + 5).compare(
            expr.op("goofy")(5)
        )

    def test_override_builtin(self):
        c1 = Column('foo', Integer,
                comparator_factory=self._add_override_factory())
        self._assert_add_override(c1)

    def test_column_proxy(self):
        t = Table('t', MetaData(),
                Column('foo', Integer,
                    comparator_factory=self._add_override_factory()))
        proxied = t.select().c.foo
        self._assert_add_override(proxied)

    def test_binary_propagate(self):
        c1 = Column('foo', Integer,
                comparator_factory=self._add_override_factory())

        self._assert_add_override(c1 - 6)

    def test_binary_multi_propagate(self):
        c1 = Column('foo', Integer,
                comparator_factory=self._add_override_factory())
        self._assert_add_override((c1 - 6) + 5)

    def test_no_boolean_propagate(self):
        c1 = Column('foo', Integer,
                comparator_factory=self._add_override_factory())

        self._assert_not_add_override(c1 == 56)

