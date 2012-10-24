from sqlalchemy.testing import fixtures
from sqlalchemy import testing
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.sql import column, desc, asc, literal, collate
from sqlalchemy.sql.expression import BinaryExpression, \
                ClauseList, Grouping, \
                UnaryExpression, select, union
from sqlalchemy.sql import operators, table
from sqlalchemy import String, Integer
from sqlalchemy import exc
from sqlalchemy.schema import Column, Table, MetaData
from sqlalchemy.types import TypeEngine, TypeDecorator, UserDefinedType
from sqlalchemy.dialects import mysql, firebird

from sqlalchemy import text, literal_column

class DefaultColumnComparatorTest(fixtures.TestBase):

    def _do_scalar_test(self, operator, compare_to):
        left = column('left')
        assert left.comparator.operate(operator).compare(
            compare_to(left)
        )

    def _do_operate_test(self, operator, right=column('right')):
        left = column('left')

        assert left.comparator.operate(operator, right).compare(
            BinaryExpression(left, right, operator)
        )

        assert operator(left, right).compare(
            BinaryExpression(left, right, operator)
        )

    def test_desc(self):
        self._do_scalar_test(operators.desc_op, desc)

    def test_asc(self):
        self._do_scalar_test(operators.asc_op, asc)

    def test_plus(self):
        self._do_operate_test(operators.add)

    def test_is_null(self):
        self._do_operate_test(operators.is_, None)

    def test_isnot_null(self):
        self._do_operate_test(operators.isnot, None)

    def test_is(self):
        self._do_operate_test(operators.is_)

    def test_isnot(self):
        self._do_operate_test(operators.isnot)

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

class ExtensionOperatorTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_contains(self):
        class MyType(UserDefinedType):
            class comparator_factory(UserDefinedType.Comparator):
                def contains(self, other, **kw):
                    return self.op("->")(other)

        self.assert_compile(
            Column('x', MyType()).contains(5),
            "x -> :x_1"
        )

    def test_getitem(self):
        class MyType(UserDefinedType):
            class comparator_factory(UserDefinedType.Comparator):
                def __getitem__(self, index):
                    return self.op("->")(index)

        self.assert_compile(
            Column('x', MyType())[5],
            "x -> :x_1"
        )

    def test_lshift(self):
        class MyType(UserDefinedType):
            class comparator_factory(UserDefinedType.Comparator):
                def __lshift__(self, other):
                    return self.op("->")(other)

        self.assert_compile(
            Column('x', MyType()) << 5,
            "x -> :x_1"
        )

    def test_rshift(self):
        class MyType(UserDefinedType):
            class comparator_factory(UserDefinedType.Comparator):
                def __rshift__(self, other):
                    return self.op("->")(other)

        self.assert_compile(
            Column('x', MyType()) >> 5,
            "x -> :x_1"
        )

from sqlalchemy import and_, not_, between

class OperatorPrecedenceTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table = table('op', column('field'))

    def test_operator_precedence_1(self):
        self.assert_compile(
                    self.table.select((self.table.c.field == 5) == None),
            "SELECT op.field FROM op WHERE (op.field = :field_1) IS NULL")

    def test_operator_precedence_2(self):
        self.assert_compile(
                self.table.select(
                        (self.table.c.field + 5) == self.table.c.field),
            "SELECT op.field FROM op WHERE op.field + :field_1 = op.field")

    def test_operator_precedence_3(self):
        self.assert_compile(
                    self.table.select((self.table.c.field + 5) * 6),
            "SELECT op.field FROM op WHERE (op.field + :field_1) * :param_1")

    def test_operator_precedence_4(self):
        self.assert_compile(self.table.select((self.table.c.field * 5) + 6),
            "SELECT op.field FROM op WHERE op.field * :field_1 + :param_1")

    def test_operator_precedence_5(self):
        self.assert_compile(self.table.select(
                            5 + self.table.c.field.in_([5, 6])),
            "SELECT op.field FROM op WHERE :param_1 + "
                        "(op.field IN (:field_1, :field_2))")

    def test_operator_precedence_6(self):
        self.assert_compile(self.table.select(
                        (5 + self.table.c.field).in_([5, 6])),
            "SELECT op.field FROM op WHERE :field_1 + op.field "
                    "IN (:param_1, :param_2)")

    def test_operator_precedence_7(self):
        self.assert_compile(self.table.select(
                    not_(and_(self.table.c.field == 5,
                        self.table.c.field == 7))),
            "SELECT op.field FROM op WHERE NOT "
                "(op.field = :field_1 AND op.field = :field_2)")

    def test_operator_precedence_8(self):
        self.assert_compile(self.table.select(not_(self.table.c.field == 5)),
            "SELECT op.field FROM op WHERE op.field != :field_1")

    def test_operator_precedence_9(self):
        self.assert_compile(self.table.select(
                        not_(self.table.c.field.between(5, 6))),
            "SELECT op.field FROM op WHERE NOT "
                    "(op.field BETWEEN :field_1 AND :field_2)")

    def test_operator_precedence_10(self):
        self.assert_compile(self.table.select(not_(self.table.c.field) == 5),
            "SELECT op.field FROM op WHERE (NOT op.field) = :param_1")

    def test_operator_precedence_11(self):
        self.assert_compile(self.table.select(
                        (self.table.c.field == self.table.c.field).\
                            between(False, True)),
            "SELECT op.field FROM op WHERE (op.field = op.field) "
                            "BETWEEN :param_1 AND :param_2")

    def test_operator_precedence_12(self):
        self.assert_compile(self.table.select(
                        between((self.table.c.field == self.table.c.field),
                                    False, True)),
            "SELECT op.field FROM op WHERE (op.field = op.field) "
                    "BETWEEN :param_1 AND :param_2")

    def test_operator_precedence_13(self):
        self.assert_compile(self.table.select(
                        self.table.c.field.match(
                                self.table.c.field).is_(None)),
            "SELECT op.field FROM op WHERE (op.field MATCH op.field) IS NULL")

class OperatorAssociativityTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_associativity_1(self):
        f = column('f')
        self.assert_compile(f - f, "f - f")

    def test_associativity_2(self):
        f = column('f')
        self.assert_compile(f - f - f, "(f - f) - f")

    def test_associativity_3(self):
        f = column('f')
        self.assert_compile((f - f) - f, "(f - f) - f")

    def test_associativity_4(self):
        f = column('f')
        self.assert_compile((f - f).label('foo') - f, "(f - f) - f")


    def test_associativity_5(self):
        f = column('f')
        self.assert_compile(f - (f - f), "f - (f - f)")

    def test_associativity_6(self):
        f = column('f')
        self.assert_compile(f - (f - f).label('foo'), "f - (f - f)")

    def test_associativity_7(self):
        f = column('f')
        # because - less precedent than /
        self.assert_compile(f / (f - f), "f / (f - f)")

    def test_associativity_8(self):
        f = column('f')
        self.assert_compile(f / (f - f).label('foo'), "f / (f - f)")

    def test_associativity_9(self):
        f = column('f')
        self.assert_compile(f / f - f, "f / f - f")

    def test_associativity_10(self):
        f = column('f')
        self.assert_compile((f / f) - f, "f / f - f")

    def test_associativity_11(self):
        f = column('f')
        self.assert_compile((f / f).label('foo') - f, "f / f - f")

    def test_associativity_12(self):
        f = column('f')
        # because / more precedent than -
        self.assert_compile(f - (f / f), "f - f / f")

    def test_associativity_13(self):
        f = column('f')
        self.assert_compile(f - (f / f).label('foo'), "f - f / f")

    def test_associativity_14(self):
        f = column('f')
        self.assert_compile(f - f / f, "f - f / f")

    def test_associativity_15(self):
        f = column('f')
        self.assert_compile((f - f) / f, "(f - f) / f")

    def test_associativity_16(self):
        f = column('f')
        self.assert_compile(((f - f) / f) - f, "(f - f) / f - f")

    def test_associativity_17(self):
        f = column('f')
        self.assert_compile((f - f) / (f - f), "(f - f) / (f - f)")

    def test_associativity_18(self):
        f = column('f')
        # higher precedence
        self.assert_compile((f / f) - (f / f), "f / f - f / f")

    def test_associativity_19(self):
        f = column('f')
        self.assert_compile((f / f) - (f - f), "f / f - (f - f)")

    def test_associativity_20(self):
        f = column('f')
        self.assert_compile((f / f) / (f - f), "(f / f) / (f - f)")

    def test_associativity_21(self):
        f = column('f')
        self.assert_compile(f / (f / (f - f)), "f / (f / (f - f))")

class InTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
        column('myid', Integer),
        column('name', String),
        column('description', String),
    )
    table2 = table(
        'myothertable',
        column('otherid', Integer),
        column('othername', String),
    )

    def test_in_1(self):
        self.assert_compile(self.table1.c.myid.in_(['a']),
        "mytable.myid IN (:myid_1)")

    def test_in_2(self):
        self.assert_compile(~self.table1.c.myid.in_(['a']),
        "mytable.myid NOT IN (:myid_1)")

    def test_in_3(self):
        self.assert_compile(self.table1.c.myid.in_(['a', 'b']),
        "mytable.myid IN (:myid_1, :myid_2)")

    def test_in_4(self):
        self.assert_compile(self.table1.c.myid.in_(iter(['a', 'b'])),
        "mytable.myid IN (:myid_1, :myid_2)")

    def test_in_5(self):
        self.assert_compile(self.table1.c.myid.in_([literal('a')]),
        "mytable.myid IN (:param_1)")

    def test_in_6(self):
        self.assert_compile(self.table1.c.myid.in_([literal('a'), 'b']),
        "mytable.myid IN (:param_1, :myid_1)")

    def test_in_7(self):
        self.assert_compile(
                self.table1.c.myid.in_([literal('a'), literal('b')]),
        "mytable.myid IN (:param_1, :param_2)")

    def test_in_8(self):
        self.assert_compile(self.table1.c.myid.in_(['a', literal('b')]),
        "mytable.myid IN (:myid_1, :param_1)")

    def test_in_9(self):
        self.assert_compile(self.table1.c.myid.in_([literal(1) + 'a']),
        "mytable.myid IN (:param_1 + :param_2)")

    def test_in_10(self):
        self.assert_compile(self.table1.c.myid.in_([literal('a') + 'a', 'b']),
        "mytable.myid IN (:param_1 || :param_2, :myid_1)")

    def test_in_11(self):
        self.assert_compile(self.table1.c.myid.in_([literal('a') + \
                                            literal('a'), literal('b')]),
        "mytable.myid IN (:param_1 || :param_2, :param_3)")

    def test_in_12(self):
        self.assert_compile(self.table1.c.myid.in_([1, literal(3) + 4]),
        "mytable.myid IN (:myid_1, :param_1 + :param_2)")

    def test_in_13(self):
        self.assert_compile(self.table1.c.myid.in_([literal('a') < 'b']),
        "mytable.myid IN (:param_1 < :param_2)")

    def test_in_14(self):
        self.assert_compile(self.table1.c.myid.in_([self.table1.c.myid]),
        "mytable.myid IN (mytable.myid)")

    def test_in_15(self):
        self.assert_compile(self.table1.c.myid.in_(['a', self.table1.c.myid]),
        "mytable.myid IN (:myid_1, mytable.myid)")

    def test_in_16(self):
        self.assert_compile(self.table1.c.myid.in_([literal('a'),
                                    self.table1.c.myid]),
        "mytable.myid IN (:param_1, mytable.myid)")

    def test_in_17(self):
        self.assert_compile(self.table1.c.myid.in_([literal('a'), \
                                    self.table1.c.myid + 'a']),
        "mytable.myid IN (:param_1, mytable.myid + :myid_1)")

    def test_in_18(self):
        self.assert_compile(self.table1.c.myid.in_([literal(1), 'a' + \
                            self.table1.c.myid]),
        "mytable.myid IN (:param_1, :myid_1 + mytable.myid)")

    def test_in_19(self):
        self.assert_compile(self.table1.c.myid.in_([1, 2, 3]),
        "mytable.myid IN (:myid_1, :myid_2, :myid_3)")

    def test_in_20(self):
        self.assert_compile(self.table1.c.myid.in_(
                                select([self.table2.c.otherid])),
        "mytable.myid IN (SELECT myothertable.otherid FROM myothertable)")

    def test_in_21(self):
        self.assert_compile(~self.table1.c.myid.in_(
                            select([self.table2.c.otherid])),
        "mytable.myid NOT IN (SELECT myothertable.otherid FROM myothertable)")

    def test_in_22(self):
        self.assert_compile(
                self.table1.c.myid.in_(
                        text("SELECT myothertable.otherid FROM myothertable")
                    ),
                    "mytable.myid IN (SELECT myothertable.otherid "
                    "FROM myothertable)"
        )

    @testing.emits_warning('.*empty sequence.*')
    def test_in_23(self):
        self.assert_compile(self.table1.c.myid.in_([]),
        "mytable.myid != mytable.myid")

    def test_in_24(self):
        self.assert_compile(
            select([self.table1.c.myid.in_(select([self.table2.c.otherid]))]),
            "SELECT mytable.myid IN (SELECT myothertable.otherid "
                "FROM myothertable) AS anon_1 FROM mytable"
        )

    def test_in_25(self):
        self.assert_compile(
            select([self.table1.c.myid.in_(
                        select([self.table2.c.otherid]).as_scalar())]),
            "SELECT mytable.myid IN (SELECT myothertable.otherid "
                "FROM myothertable) AS anon_1 FROM mytable"
        )

    def test_in_26(self):
        self.assert_compile(self.table1.c.myid.in_(
            union(
                  select([self.table1.c.myid], self.table1.c.myid == 5),
                  select([self.table1.c.myid], self.table1.c.myid == 12),
            )
        ), "mytable.myid IN ("\
        "SELECT mytable.myid FROM mytable WHERE mytable.myid = :myid_1 "\
        "UNION SELECT mytable.myid FROM mytable WHERE mytable.myid = :myid_2)")

    def test_in_27(self):
        # test that putting a select in an IN clause does not
        # blow away its ORDER BY clause
        self.assert_compile(
            select([self.table1, self.table2],
                self.table2.c.otherid.in_(
                    select([self.table2.c.otherid],
                                    order_by=[self.table2.c.othername],
                                    limit=10, correlate=False)
                ),
                from_obj=[self.table1.join(self.table2,
                            self.table1.c.myid == self.table2.c.otherid)],
                order_by=[self.table1.c.myid]
            ),
            "SELECT mytable.myid, mytable.name, mytable.description, "
            "myothertable.otherid, myothertable.othername FROM mytable "\
            "JOIN myothertable ON mytable.myid = myothertable.otherid "
            "WHERE myothertable.otherid IN (SELECT myothertable.otherid "\
            "FROM myothertable ORDER BY myothertable.othername "
            "LIMIT :param_1) ORDER BY mytable.myid",
            {'param_1': 10}
        )


class ComposedLikeOperatorsTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_contains(self):
        self.assert_compile(
            column('x').contains('y'),
            "x LIKE '%%' || :x_1 || '%%'",
            checkparams={'x_1': 'y'}
        )

    def test_contains_escape(self):
        self.assert_compile(
            column('x').contains('y', escape='\\'),
            "x LIKE '%%' || :x_1 || '%%' ESCAPE '\\'",
            checkparams={'x_1': 'y'}
        )

    def test_contains_literal(self):
        self.assert_compile(
            column('x').contains(literal_column('y')),
            "x LIKE '%%' || y || '%%'",
            checkparams={}
        )

    def test_contains_text(self):
        self.assert_compile(
            column('x').contains(text('y')),
            "x LIKE '%%' || y || '%%'",
            checkparams={}
        )

    def test_not_contains(self):
        self.assert_compile(
            ~column('x').contains('y'),
            "x NOT LIKE '%%' || :x_1 || '%%'",
            checkparams={'x_1': 'y'}
        )

    def test_not_contains_escape(self):
        self.assert_compile(
            ~column('x').contains('y', escape='\\'),
            "x NOT LIKE '%%' || :x_1 || '%%' ESCAPE '\\'",
            checkparams={'x_1': 'y'}
        )

    def test_contains_concat(self):
        self.assert_compile(
            column('x').contains('y'),
            "x LIKE concat(concat('%%', %s), '%%')",
            checkparams={'x_1': 'y'},
            dialect=mysql.dialect()
        )

    def test_not_contains_concat(self):
        self.assert_compile(
            ~column('x').contains('y'),
            "x NOT LIKE concat(concat('%%', %s), '%%')",
            checkparams={'x_1': 'y'},
            dialect=mysql.dialect()
        )

    def test_contains_literal_concat(self):
        self.assert_compile(
            column('x').contains(literal_column('y')),
            "x LIKE concat(concat('%%', y), '%%')",
            checkparams={},
            dialect=mysql.dialect()
        )

    def test_contains_text_concat(self):
        self.assert_compile(
            column('x').contains(text('y')),
            "x LIKE concat(concat('%%', y), '%%')",
            checkparams={},
            dialect=mysql.dialect()
        )

    def test_startswith(self):
        self.assert_compile(
            column('x').startswith('y'),
            "x LIKE :x_1 || '%%'",
            checkparams={'x_1': 'y'}
        )

    def test_startswith_escape(self):
        self.assert_compile(
            column('x').startswith('y', escape='\\'),
            "x LIKE :x_1 || '%%' ESCAPE '\\'",
            checkparams={'x_1': 'y'}
        )

    def test_not_startswith(self):
        self.assert_compile(
            ~column('x').startswith('y'),
            "x NOT LIKE :x_1 || '%%'",
            checkparams={'x_1': 'y'}
        )

    def test_not_startswith_escape(self):
        self.assert_compile(
            ~column('x').startswith('y', escape='\\'),
            "x NOT LIKE :x_1 || '%%' ESCAPE '\\'",
            checkparams={'x_1': 'y'}
        )

    def test_startswith_literal(self):
        self.assert_compile(
            column('x').startswith(literal_column('y')),
            "x LIKE y || '%%'",
            checkparams={}
        )

    def test_startswith_text(self):
        self.assert_compile(
            column('x').startswith(text('y')),
            "x LIKE y || '%%'",
            checkparams={}
        )

    def test_startswith_concat(self):
        self.assert_compile(
            column('x').startswith('y'),
            "x LIKE concat(%s, '%%')",
            checkparams={'x_1': 'y'},
            dialect=mysql.dialect()
        )

    def test_not_startswith_concat(self):
        self.assert_compile(
            ~column('x').startswith('y'),
            "x NOT LIKE concat(%s, '%%')",
            checkparams={'x_1': 'y'},
            dialect=mysql.dialect()
        )

    def test_startswith_firebird(self):
        self.assert_compile(
            column('x').startswith('y'),
            "x STARTING WITH :x_1",
            checkparams={'x_1': 'y'},
            dialect=firebird.dialect()
        )

    def test_not_startswith_firebird(self):
        self.assert_compile(
            ~column('x').startswith('y'),
            "x NOT STARTING WITH :x_1",
            checkparams={'x_1': 'y'},
            dialect=firebird.dialect()
        )

    def test_startswith_literal_mysql(self):
        self.assert_compile(
            column('x').startswith(literal_column('y')),
            "x LIKE concat(y, '%%')",
            checkparams={},
            dialect=mysql.dialect()
        )

    def test_startswith_text_mysql(self):
        self.assert_compile(
            column('x').startswith(text('y')),
            "x LIKE concat(y, '%%')",
            checkparams={},
            dialect=mysql.dialect()
        )

    def test_endswith(self):
        self.assert_compile(
            column('x').endswith('y'),
            "x LIKE '%%' || :x_1",
            checkparams={'x_1': 'y'}
        )

    def test_endswith_escape(self):
        self.assert_compile(
            column('x').endswith('y', escape='\\'),
            "x LIKE '%%' || :x_1 ESCAPE '\\'",
            checkparams={'x_1': 'y'}
        )

    def test_not_endswith(self):
        self.assert_compile(
            ~column('x').endswith('y'),
            "x NOT LIKE '%%' || :x_1",
            checkparams={'x_1': 'y'}
        )

    def test_not_endswith_escape(self):
        self.assert_compile(
            ~column('x').endswith('y', escape='\\'),
            "x NOT LIKE '%%' || :x_1 ESCAPE '\\'",
            checkparams={'x_1': 'y'}
        )

    def test_endswith_literal(self):
        self.assert_compile(
            column('x').endswith(literal_column('y')),
            "x LIKE '%%' || y",
            checkparams={}
        )

    def test_endswith_text(self):
        self.assert_compile(
            column('x').endswith(text('y')),
            "x LIKE '%%' || y",
            checkparams={}
        )

    def test_endswith_mysql(self):
        self.assert_compile(
            column('x').endswith('y'),
            "x LIKE concat('%%', %s)",
            checkparams={'x_1': 'y'},
            dialect=mysql.dialect()
        )

    def test_not_endswith_mysql(self):
        self.assert_compile(
            ~column('x').endswith('y'),
            "x NOT LIKE concat('%%', %s)",
            checkparams={'x_1': 'y'},
            dialect=mysql.dialect()
        )

    def test_endswith_literal_mysql(self):
        self.assert_compile(
            column('x').endswith(literal_column('y')),
            "x LIKE concat('%%', y)",
            checkparams={},
            dialect=mysql.dialect()
        )

    def test_endswith_text_mysql(self):
        self.assert_compile(
            column('x').endswith(text('y')),
            "x LIKE concat('%%', y)",
            checkparams={},
            dialect=mysql.dialect()
        )

