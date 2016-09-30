from sqlalchemy.testing import fixtures, eq_, is_, is_not_
from sqlalchemy import testing
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.sql import column, desc, asc, literal, collate, null, \
    true, false, any_, all_
from sqlalchemy.sql.expression import BinaryExpression, \
    ClauseList, Grouping, \
    UnaryExpression, select, union, func, tuple_
from sqlalchemy.sql import operators, table
import operator
from sqlalchemy import String, Integer, LargeBinary
from sqlalchemy import exc
from sqlalchemy.engine import default
from sqlalchemy.sql.elements import _literal_as_text
from sqlalchemy.schema import Column, Table, MetaData
from sqlalchemy.sql import compiler
from sqlalchemy.types import TypeEngine, TypeDecorator, UserDefinedType, \
    Boolean, NullType, MatchType, Indexable, Concatenable, ARRAY, JSON, \
    DateTime
from sqlalchemy.dialects import mysql, firebird, postgresql, oracle, \
    sqlite, mssql
from sqlalchemy import util
import datetime
import collections
from sqlalchemy import text, literal_column
from sqlalchemy import and_, not_, between, or_


class LoopOperate(operators.ColumnOperators):

    def operate(self, op, *other, **kwargs):
        return op


class DefaultColumnComparatorTest(fixtures.TestBase):

    def _do_scalar_test(self, operator, compare_to):
        left = column('left')
        assert left.comparator.operate(operator).compare(
            compare_to(left)
        )
        self._loop_test(operator)

    def _do_operate_test(self, operator, right=column('right')):
        left = column('left')

        assert left.comparator.operate(
            operator,
            right).compare(
            BinaryExpression(
                _literal_as_text(left),
                _literal_as_text(right),
                operator))

        assert operator(
            left,
            right).compare(
            BinaryExpression(
                _literal_as_text(left),
                _literal_as_text(right),
                operator))

        self._loop_test(operator, right)

    def _loop_test(self, operator, *arg):
        l = LoopOperate()
        is_(
            operator(l, *arg),
            operator
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

    def test_is_null_const(self):
        self._do_operate_test(operators.is_, null())

    def test_is_true_const(self):
        self._do_operate_test(operators.is_, true())

    def test_is_false_const(self):
        self._do_operate_test(operators.is_, false())

    def test_equals_true(self):
        self._do_operate_test(operators.eq, True)

    def test_notequals_true(self):
        self._do_operate_test(operators.ne, True)

    def test_is_distinct_from_true(self):
        self._do_operate_test(operators.is_distinct_from, True)

    def test_is_distinct_from_false(self):
        self._do_operate_test(operators.is_distinct_from, False)

    def test_is_distinct_from_null(self):
        self._do_operate_test(operators.is_distinct_from, None)

    def test_isnot_distinct_from_true(self):
        self._do_operate_test(operators.isnot_distinct_from, True)

    def test_is_true(self):
        self._do_operate_test(operators.is_, True)

    def test_isnot_true(self):
        self._do_operate_test(operators.isnot, True)

    def test_is_false(self):
        self._do_operate_test(operators.is_, False)

    def test_isnot_false(self):
        self._do_operate_test(operators.isnot, False)

    def test_like(self):
        self._do_operate_test(operators.like_op)

    def test_notlike(self):
        self._do_operate_test(operators.notlike_op)

    def test_ilike(self):
        self._do_operate_test(operators.ilike_op)

    def test_notilike(self):
        self._do_operate_test(operators.notilike_op)

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
        self._loop_test(operators.in_op, [1, 2, 3])

    def test_notin(self):
        left = column('left')
        assert left.comparator.operate(operators.notin_op, [1, 2, 3]).compare(
            BinaryExpression(
                left,
                Grouping(ClauseList(
                    literal(1), literal(2), literal(3)
                )),
                operators.notin_op
            )
        )
        self._loop_test(operators.notin_op, [1, 2, 3])

    def test_in_no_accept_list_of_non_column_element(self):
        left = column('left')
        foo = ClauseList()
        assert_raises_message(
            exc.InvalidRequestError,
            r"in_\(\) accepts either a list of expressions or a selectable:",
            left.in_, [foo]
        )

    def test_in_no_accept_non_list_non_selectable(self):
        left = column('left')
        right = column('right')
        assert_raises_message(
            exc.InvalidRequestError,
            r"in_\(\) accepts either a list of expressions or a selectable:",
            left.in_, right
        )

    def test_in_no_accept_non_list_thing_with_getitem(self):
        # test [ticket:2726]
        class HasGetitem(String):

            class comparator_factory(String.Comparator):

                def __getitem__(self, value):
                    return value

        left = column('left')
        right = column('right', HasGetitem)
        assert_raises_message(
            exc.InvalidRequestError,
            r"in_\(\) accepts either a list of expressions or a selectable:",
            left.in_, right
        )

    def test_collate(self):
        left = column('left')
        right = "some collation"
        left.comparator.operate(operators.collate, right).compare(
            collate(left, right)
        )

    def test_concat(self):
        self._do_operate_test(operators.concat_op)

    def test_default_adapt(self):
        class TypeOne(TypeEngine):
            pass

        class TypeTwo(TypeEngine):
            pass

        expr = column('x', TypeOne()) - column('y', TypeTwo())
        is_(
            expr.type._type_affinity, TypeOne
        )

    def test_concatenable_adapt(self):
        class TypeOne(Concatenable, TypeEngine):
            pass

        class TypeTwo(Concatenable, TypeEngine):
            pass

        class TypeThree(TypeEngine):
            pass

        expr = column('x', TypeOne()) - column('y', TypeTwo())
        is_(
            expr.type._type_affinity, TypeOne
        )
        is_(
            expr.operator, operator.sub
        )

        expr = column('x', TypeOne()) + column('y', TypeTwo())
        is_(
            expr.type._type_affinity, TypeOne
        )
        is_(
            expr.operator, operators.concat_op
        )

        expr = column('x', TypeOne()) - column('y', TypeThree())
        is_(
            expr.type._type_affinity, TypeOne
        )
        is_(
            expr.operator, operator.sub
        )

        expr = column('x', TypeOne()) + column('y', TypeThree())
        is_(
            expr.type._type_affinity, TypeOne
        )
        is_(
            expr.operator, operator.add
        )

    def test_contains_override_raises(self):
        for col in [
            Column('x', String),
            Column('x', Integer),
            Column('x', DateTime)
        ]:
            assert_raises_message(
                NotImplementedError,
                "Operator 'contains' is not supported on this expression",
                lambda: 'foo' in col
            )


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

                def __invert__(self):
                    return UnaryExpression(self.expr,
                                           operator=operators.custom_op("!!!"),
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

    def test_factorial_invert(self):
        col = column('somecol', self._factorial_fixture())
        self.assert_compile(
            ~col,
            "!!! somecol"
        )

    def test_double_factorial_invert(self):
        col = column('somecol', self._factorial_fixture())
        self.assert_compile(
            ~(~col),
            "!!! (!!! somecol)"
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
        self._assert_and_override(proxied)

    def test_alias_proxy(self):
        t = Table('t', MetaData(),
                  Column('foo', self._add_override_factory())
                  )
        proxied = t.alias().c.foo
        self._assert_add_override(proxied)
        self._assert_and_override(proxied)

    def test_binary_propagate(self):
        c1 = Column('foo', self._add_override_factory())
        self._assert_add_override(c1 - 6)
        self._assert_and_override(c1 - 6)

    def test_reverse_binary_propagate(self):
        c1 = Column('foo', self._add_override_factory())
        self._assert_add_override(6 - c1)
        self._assert_and_override(6 - c1)

    def test_binary_multi_propagate(self):
        c1 = Column('foo', self._add_override_factory())
        self._assert_add_override((c1 - 6) + 5)
        self._assert_and_override((c1 - 6) + 5)

    def test_no_boolean_propagate(self):
        c1 = Column('foo', self._add_override_factory())
        self._assert_not_add_override(c1 == 56)
        self._assert_not_and_override(c1 == 56)

    def _assert_and_override(self, expr):
        assert (expr & text("5")).compare(
            expr.op("goofy_and")(text("5"))
        )

    def _assert_add_override(self, expr):
        assert (expr + 5).compare(
            expr.op("goofy")(5)
        )

    def _assert_not_add_override(self, expr):
        assert not (expr + 5).compare(
            expr.op("goofy")(5)
        )

    def _assert_not_and_override(self, expr):
        assert not (expr & text("5")).compare(
            expr.op("goofy_and")(text("5"))
        )


class CustomComparatorTest(_CustomComparatorTests, fixtures.TestBase):

    def _add_override_factory(self):

        class MyInteger(Integer):

            class comparator_factory(TypeEngine.Comparator):

                def __init__(self, expr):
                    super(MyInteger.comparator_factory, self).__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        return MyInteger


class TypeDecoratorComparatorTest(_CustomComparatorTests, fixtures.TestBase):

    def _add_override_factory(self):

        class MyInteger(TypeDecorator):
            impl = Integer

            class comparator_factory(TypeDecorator.Comparator):

                def __init__(self, expr):
                    super(MyInteger.comparator_factory, self).__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        return MyInteger


class TypeDecoratorTypeDecoratorComparatorTest(
        _CustomComparatorTests, fixtures.TestBase):

    def _add_override_factory(self):

        class MyIntegerOne(TypeDecorator):
            impl = Integer

            class comparator_factory(TypeDecorator.Comparator):

                def __init__(self, expr):
                    super(MyIntegerOne.comparator_factory, self).__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        class MyIntegerTwo(TypeDecorator):
            impl = MyIntegerOne

        return MyIntegerTwo


class TypeDecoratorWVariantComparatorTest(
        _CustomComparatorTests,
        fixtures.TestBase):

    def _add_override_factory(self):

        class SomeOtherInteger(Integer):

            class comparator_factory(TypeEngine.Comparator):

                def __init__(self, expr):
                    super(
                        SomeOtherInteger.comparator_factory,
                        self).__init__(expr)

                def __add__(self, other):
                    return self.expr.op("not goofy")(other)

                def __and__(self, other):
                    return self.expr.op("not goofy_and")(other)

        class MyInteger(TypeDecorator):
            impl = Integer

            class comparator_factory(TypeDecorator.Comparator):

                def __init__(self, expr):
                    super(MyInteger.comparator_factory, self).__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        return MyInteger().with_variant(SomeOtherInteger, "mysql")


class CustomEmbeddedinTypeDecoratorTest(
        _CustomComparatorTests,
        fixtures.TestBase):

    def _add_override_factory(self):
        class MyInteger(Integer):

            class comparator_factory(TypeEngine.Comparator):

                def __init__(self, expr):
                    super(MyInteger.comparator_factory, self).__init__(expr)

                def __add__(self, other):
                    return self.expr.op("goofy")(other)

                def __and__(self, other):
                    return self.expr.op("goofy_and")(other)

        class MyDecInteger(TypeDecorator):
            impl = MyInteger

        return MyDecInteger


class NewOperatorTest(_CustomComparatorTests, fixtures.TestBase):

    def _add_override_factory(self):
        class MyInteger(Integer):

            class comparator_factory(TypeEngine.Comparator):

                def __init__(self, expr):
                    super(MyInteger.comparator_factory, self).__init__(expr)

                def foob(self, other):
                    return self.expr.op("foob")(other)
        return MyInteger

    def _assert_add_override(self, expr):
        assert (expr.foob(5)).compare(
            expr.op("foob")(5)
        )

    def _assert_not_add_override(self, expr):
        assert not hasattr(expr, "foob")

    def _assert_and_override(self, expr):
        pass

    def _assert_not_and_override(self, expr):
        pass


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

    def test_op_not_an_iterator(self):
        # see [ticket:2726]
        class MyType(UserDefinedType):

            class comparator_factory(UserDefinedType.Comparator):

                def __getitem__(self, index):
                    return self.op("->")(index)

        col = Column('x', MyType())
        assert not isinstance(col, collections.Iterable)

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


class JSONIndexOpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    def setUp(self):
        class MyTypeCompiler(compiler.GenericTypeCompiler):
            def visit_mytype(self, type, **kw):
                return "MYTYPE"

            def visit_myothertype(self, type, **kw):
                return "MYOTHERTYPE"

        class MyCompiler(compiler.SQLCompiler):

            def visit_json_getitem_op_binary(self, binary, operator, **kw):
                return self._generate_generic_binary(
                    binary, " -> ", eager_grouping=True, **kw
                )

            def visit_json_path_getitem_op_binary(
                    self, binary, operator, **kw):
                return self._generate_generic_binary(
                    binary, " #> ", eager_grouping=True, **kw
                )

            def visit_getitem_binary(self, binary, operator, **kw):
                raise NotImplementedError()

        class MyDialect(default.DefaultDialect):
            statement_compiler = MyCompiler
            type_compiler = MyTypeCompiler

        class MyType(JSON):
            __visit_name__ = 'mytype'

            pass

        self.MyType = MyType
        self.__dialect__ = MyDialect()

    def test_setup_getitem(self):
        col = Column('x', self.MyType())

        is_(
            col[5].type._type_affinity, JSON
        )
        is_(
            col[5]['foo'].type._type_affinity, JSON
        )
        is_(
            col[('a', 'b', 'c')].type._type_affinity, JSON
        )

    def test_getindex_literal_integer(self):

        col = Column('x', self.MyType())

        self.assert_compile(
            col[5],
            "x -> :x_1",
            checkparams={'x_1': 5}
        )

    def test_getindex_literal_string(self):

        col = Column('x', self.MyType())

        self.assert_compile(
            col['foo'],
            "x -> :x_1",
            checkparams={'x_1': 'foo'}
        )

    def test_path_getindex_literal(self):

        col = Column('x', self.MyType())

        self.assert_compile(
            col[('a', 'b', 3, 4, 'd')],
            "x #> :x_1",
            checkparams={'x_1': ('a', 'b', 3, 4, 'd')}
        )

    def test_getindex_sqlexpr(self):

        col = Column('x', self.MyType())
        col2 = Column('y', Integer())

        self.assert_compile(
            col[col2],
            "x -> y",
            checkparams={}
        )

    def test_getindex_sqlexpr_right_grouping(self):

        col = Column('x', self.MyType())
        col2 = Column('y', Integer())

        self.assert_compile(
            col[col2 + 8],
            "x -> (y + :y_1)",
            checkparams={'y_1': 8}
        )

    def test_getindex_sqlexpr_left_grouping(self):

        col = Column('x', self.MyType())

        self.assert_compile(
            col[8] != None,
            "(x -> :x_1) IS NOT NULL"
        )

    def test_getindex_sqlexpr_both_grouping(self):

        col = Column('x', self.MyType())
        col2 = Column('y', Integer())

        self.assert_compile(
            col[col2 + 8] != None,
            "(x -> (y + :y_1)) IS NOT NULL",
            checkparams={'y_1': 8}
        )

    def test_override_operators(self):
        special_index_op = operators.custom_op('$$>')

        class MyOtherType(JSON, TypeEngine):
            __visit_name__ = 'myothertype'

            class Comparator(TypeEngine.Comparator):

                def _adapt_expression(self, op, other_comparator):
                    return special_index_op, MyOtherType()

            comparator_factory = Comparator

        col = Column('x', MyOtherType())
        self.assert_compile(
            col[5],
            "x $$> :x_1",
            checkparams={'x_1': 5}
        )


class ArrayIndexOpTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    def setUp(self):
        class MyTypeCompiler(compiler.GenericTypeCompiler):
            def visit_mytype(self, type, **kw):
                return "MYTYPE"

            def visit_myothertype(self, type, **kw):
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
                    self.process(binary.right, **kw)
                )

        class MyDialect(default.DefaultDialect):
            statement_compiler = MyCompiler
            type_compiler = MyTypeCompiler

        class MyType(ARRAY):
            __visit_name__ = 'mytype'

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

        col = Column('x', self.MyType(dimensions=3))

        is_(
            col[5].type._type_affinity, ARRAY
        )
        eq_(
            col[5].type.dimensions, 2
        )
        is_(
            col[5][6].type._type_affinity, ARRAY
        )
        eq_(
            col[5][6].type.dimensions, 1
        )
        is_(
            col[5][6][7].type._type_affinity, Integer
        )

    def test_getindex_literal(self):

        col = Column('x', self.MyType())

        self.assert_compile(
            col[5],
            "x[:x_1]",
            checkparams={'x_1': 5}
        )

    def test_contains_override_raises(self):
        col = Column('x', self.MyType())

        assert_raises_message(
            NotImplementedError,
            "Operator 'contains' is not supported on this expression",
            lambda: 'foo' in col
        )

    def test_getindex_sqlexpr(self):

        col = Column('x', self.MyType())
        col2 = Column('y', Integer())

        self.assert_compile(
            col[col2],
            "x[y]",
            checkparams={}
        )

        self.assert_compile(
            col[col2 + 8],
            "x[(y + :y_1)]",
            checkparams={'y_1': 8}
        )

    def test_getslice_literal(self):

        col = Column('x', self.MyType())

        self.assert_compile(
            col[5:6],
            "x[:x_1::x_2]",
            checkparams={'x_1': 5, 'x_2': 6}
        )

    def test_getslice_sqlexpr(self):

        col = Column('x', self.MyType())
        col2 = Column('y', Integer())

        self.assert_compile(
            col[col2:col2 + 5],
            "x[y:y + :y_1]",
            checkparams={'y_1': 5}
        )

    def test_getindex_literal_zeroind(self):

        col = Column('x', self.MyType(zero_indexes=True))

        self.assert_compile(
            col[5],
            "x[:x_1]",
            checkparams={'x_1': 6}
        )

    def test_getindex_sqlexpr_zeroind(self):

        col = Column('x', self.MyType(zero_indexes=True))
        col2 = Column('y', Integer())

        self.assert_compile(
            col[col2],
            "x[(y + :y_1)]",
            checkparams={'y_1': 1}
        )

        self.assert_compile(
            col[col2 + 8],
            "x[(y + :y_1 + :param_1)]",
            checkparams={'y_1': 8, 'param_1': 1}
        )

    def test_getslice_literal_zeroind(self):

        col = Column('x', self.MyType(zero_indexes=True))

        self.assert_compile(
            col[5:6],
            "x[:x_1::x_2]",
            checkparams={'x_1': 6, 'x_2': 7}
        )

    def test_getslice_sqlexpr_zeroind(self):

        col = Column('x', self.MyType(zero_indexes=True))
        col2 = Column('y', Integer())

        self.assert_compile(
            col[col2:col2 + 5],
            "x[y + :y_1:y + :y_2 + :param_1]",
            checkparams={'y_1': 1, 'y_2': 5, 'param_1': 1}
        )

    def test_override_operators(self):
        special_index_op = operators.custom_op('->')

        class MyOtherType(Indexable, TypeEngine):
            __visit_name__ = 'myothertype'

            class Comparator(TypeEngine.Comparator):

                def _adapt_expression(self, op, other_comparator):
                    return special_index_op, MyOtherType()

            comparator_factory = Comparator

        col = Column('x', MyOtherType())
        self.assert_compile(
            col[5],
            "x -> :x_1",
            checkparams={'x_1': 5}
        )


class BooleanEvalTest(fixtures.TestBase, testing.AssertsCompiledSQL):

    """test standalone booleans being wrapped in an AsBoolean, as well
    as true/false compilation."""

    def _dialect(self, native_boolean):
        d = default.DefaultDialect()
        d.supports_native_boolean = native_boolean
        return d

    def test_one(self):
        c = column('x', Boolean)
        self.assert_compile(
            select([c]).where(c),
            "SELECT x WHERE x",
            dialect=self._dialect(True)
        )

    def test_two_a(self):
        c = column('x', Boolean)
        self.assert_compile(
            select([c]).where(c),
            "SELECT x WHERE x = 1",
            dialect=self._dialect(False)
        )

    def test_two_b(self):
        c = column('x', Boolean)
        self.assert_compile(
            select([c], whereclause=c),
            "SELECT x WHERE x = 1",
            dialect=self._dialect(False)
        )

    def test_three_a(self):
        c = column('x', Boolean)
        self.assert_compile(
            select([c]).where(~c),
            "SELECT x WHERE x = 0",
            dialect=self._dialect(False)
        )

    def test_three_b(self):
        c = column('x', Boolean)
        self.assert_compile(
            select([c], whereclause=~c),
            "SELECT x WHERE x = 0",
            dialect=self._dialect(False)
        )

    def test_four(self):
        c = column('x', Boolean)
        self.assert_compile(
            select([c]).where(~c),
            "SELECT x WHERE NOT x",
            dialect=self._dialect(True)
        )

    def test_five_a(self):
        c = column('x', Boolean)
        self.assert_compile(
            select([c]).having(c),
            "SELECT x HAVING x = 1",
            dialect=self._dialect(False)
        )

    def test_five_b(self):
        c = column('x', Boolean)
        self.assert_compile(
            select([c], having=c),
            "SELECT x HAVING x = 1",
            dialect=self._dialect(False)
        )

    def test_six(self):
        self.assert_compile(
            or_(false(), true()),
            "1 = 1",
            dialect=self._dialect(False)
        )

    def test_eight(self):
        self.assert_compile(
            and_(false(), true()),
            "false",
            dialect=self._dialect(True)
        )

    def test_nine(self):
        self.assert_compile(
            and_(false(), true()),
            "0 = 1",
            dialect=self._dialect(False)
        )

    def test_ten(self):
        c = column('x', Boolean)
        self.assert_compile(
            c == 1,
            "x = :x_1",
            dialect=self._dialect(False)
        )

    def test_eleven(self):
        c = column('x', Boolean)
        self.assert_compile(
            c.is_(true()),
            "x IS true",
            dialect=self._dialect(True)
        )

    def test_twelve(self):
        c = column('x', Boolean)
        # I don't have a solution for this one yet,
        # other than adding some heavy-handed conditionals
        # into compiler
        self.assert_compile(
            c.is_(true()),
            "x IS 1",
            dialect=self._dialect(False)
        )


class ConjunctionTest(fixtures.TestBase, testing.AssertsCompiledSQL):

    """test interaction of and_()/or_() with boolean , null constants
    """
    __dialect__ = default.DefaultDialect(supports_native_boolean=True)

    def test_one(self):
        self.assert_compile(~and_(true()), "false")

    def test_two(self):
        self.assert_compile(or_(~and_(true())), "false")

    def test_three(self):
        self.assert_compile(or_(and_()), "")

    def test_four(self):
        x = column('x')
        self.assert_compile(
            and_(or_(x == 5), or_(x == 7)),
            "x = :x_1 AND x = :x_2")

    def test_five(self):
        x = column("x")
        self.assert_compile(
            and_(true()._ifnone(None), x == 7),
            "x = :x_1"
        )

    def test_six(self):
        x = column("x")
        self.assert_compile(or_(true(), x == 7), "true")
        self.assert_compile(or_(x == 7, true()), "true")
        self.assert_compile(~or_(x == 7, true()), "false")

    def test_six_pt_five(self):
        x = column("x")
        self.assert_compile(select([x]).where(or_(x == 7, true())),
                            "SELECT x WHERE true")

        self.assert_compile(
            select(
                [x]).where(
                or_(
                    x == 7,
                    true())),
            "SELECT x WHERE 1 = 1",
            dialect=default.DefaultDialect(
                    supports_native_boolean=False))

    def test_seven(self):
        x = column("x")
        self.assert_compile(
            and_(true(), x == 7, true(), x == 9),
            "x = :x_1 AND x = :x_2")

    def test_eight(self):
        x = column("x")
        self.assert_compile(
            or_(false(), x == 7, false(), x == 9),
            "x = :x_1 OR x = :x_2")

    def test_nine(self):
        x = column("x")
        self.assert_compile(
            and_(x == 7, x == 9, false(), x == 5),
            "false"
        )
        self.assert_compile(
            ~and_(x == 7, x == 9, false(), x == 5),
            "true"
        )

    def test_ten(self):
        self.assert_compile(
            and_(None, None),
            "NULL AND NULL"
        )

    def test_eleven(self):
        x = column("x")
        self.assert_compile(
            select([x]).where(None).where(None),
            "SELECT x WHERE NULL AND NULL"
        )

    def test_twelve(self):
        x = column("x")
        self.assert_compile(
            select([x]).where(and_(None, None)),
            "SELECT x WHERE NULL AND NULL"
        )

    def test_thirteen(self):
        x = column("x")
        self.assert_compile(
            select([x]).where(~and_(None, None)),
            "SELECT x WHERE NOT (NULL AND NULL)"
        )

    def test_fourteen(self):
        x = column("x")
        self.assert_compile(
            select([x]).where(~null()),
            "SELECT x WHERE NOT NULL"
        )

    def test_constant_non_singleton(self):
        is_not_(null(), null())
        is_not_(false(), false())
        is_not_(true(), true())

    def test_constant_render_distinct(self):
        self.assert_compile(
            select([null(), null()]),
            "SELECT NULL AS anon_1, NULL AS anon_2"
        )
        self.assert_compile(
            select([true(), true()]),
            "SELECT true AS anon_1, true AS anon_2"
        )
        self.assert_compile(
            select([false(), false()]),
            "SELECT false AS anon_1, false AS anon_2"
        )

    def test_is_true_literal(self):
        c = column('x', Boolean)
        self.assert_compile(
            c.is_(True),
            "x IS true"
        )

    def test_is_false_literal(self):
        c = column('x', Boolean)
        self.assert_compile(
            c.is_(False),
            "x IS false"
        )

    def test_and_false_literal_leading(self):
        self.assert_compile(
            and_(False, True),
            "false"
        )

        self.assert_compile(
            and_(False, False),
            "false"
        )

    def test_and_true_literal_leading(self):
        self.assert_compile(
            and_(True, True),
            "true"
        )

        self.assert_compile(
            and_(True, False),
            "false"
        )

    def test_or_false_literal_leading(self):
        self.assert_compile(
            or_(False, True),
            "true"
        )

        self.assert_compile(
            or_(False, False),
            "false"
        )

    def test_or_true_literal_leading(self):
        self.assert_compile(
            or_(True, True),
            "true"
        )

        self.assert_compile(
            or_(True, False),
            "true"
        )


class OperatorPrecedenceTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
                   column('myid', Integer),
                   column('name', String),
                   column('description', String),
                   )

    table2 = table('op', column('field'))

    def test_operator_precedence_1(self):
        self.assert_compile(
            self.table2.select((self.table2.c.field == 5) == None),
            "SELECT op.field FROM op WHERE (op.field = :field_1) IS NULL")

    def test_operator_precedence_2(self):
        self.assert_compile(
            self.table2.select(
                (self.table2.c.field + 5) == self.table2.c.field),
            "SELECT op.field FROM op WHERE op.field + :field_1 = op.field")

    def test_operator_precedence_3(self):
        self.assert_compile(
            self.table2.select((self.table2.c.field + 5) * 6),
            "SELECT op.field FROM op WHERE (op.field + :field_1) * :param_1")

    def test_operator_precedence_4(self):
        self.assert_compile(
            self.table2.select(
                (self.table2.c.field * 5) + 6),
            "SELECT op.field FROM op WHERE op.field * :field_1 + :param_1")

    def test_operator_precedence_5(self):
        self.assert_compile(self.table2.select(
                            5 + self.table2.c.field.in_([5, 6])),
                            "SELECT op.field FROM op WHERE :param_1 + "
                            "(op.field IN (:field_1, :field_2))")

    def test_operator_precedence_6(self):
        self.assert_compile(self.table2.select(
            (5 + self.table2.c.field).in_([5, 6])),
            "SELECT op.field FROM op WHERE :field_1 + op.field "
            "IN (:param_1, :param_2)")

    def test_operator_precedence_7(self):
        self.assert_compile(self.table2.select(
            not_(and_(self.table2.c.field == 5,
                      self.table2.c.field == 7))),
            "SELECT op.field FROM op WHERE NOT "
            "(op.field = :field_1 AND op.field = :field_2)")

    def test_operator_precedence_8(self):
        self.assert_compile(
            self.table2.select(
                not_(
                    self.table2.c.field == 5)),
            "SELECT op.field FROM op WHERE op.field != :field_1")

    def test_operator_precedence_9(self):
        self.assert_compile(self.table2.select(
            not_(self.table2.c.field.between(5, 6))),
            "SELECT op.field FROM op WHERE "
            "op.field NOT BETWEEN :field_1 AND :field_2")

    def test_operator_precedence_10(self):
        self.assert_compile(
            self.table2.select(
                not_(
                    self.table2.c.field) == 5),
            "SELECT op.field FROM op WHERE (NOT op.field) = :param_1")

    def test_operator_precedence_11(self):
        self.assert_compile(self.table2.select(
            (self.table2.c.field == self.table2.c.field).
            between(False, True)),
            "SELECT op.field FROM op WHERE (op.field = op.field) "
            "BETWEEN :param_1 AND :param_2")

    def test_operator_precedence_12(self):
        self.assert_compile(self.table2.select(
            between((self.table2.c.field == self.table2.c.field),
                    False, True)),
            "SELECT op.field FROM op WHERE (op.field = op.field) "
            "BETWEEN :param_1 AND :param_2")

    def test_operator_precedence_13(self):
        self.assert_compile(
            self.table2.select(
                self.table2.c.field.match(
                    self.table2.c.field).is_(None)),
            "SELECT op.field FROM op WHERE (op.field MATCH op.field) IS NULL")

    def test_operator_precedence_collate_1(self):
        self.assert_compile(
            self.table1.c.name == literal('foo').collate('utf-8'),
            "mytable.name = (:param_1 COLLATE utf-8)"
        )

    def test_operator_precedence_collate_2(self):
        self.assert_compile(
            (self.table1.c.name == literal('foo')).collate('utf-8'),
            "mytable.name = :param_1 COLLATE utf-8"
        )

    def test_operator_precedence_collate_3(self):
        self.assert_compile(
            self.table1.c.name.collate('utf-8') == 'foo',
            "(mytable.name COLLATE utf-8) = :param_1"
        )

    def test_operator_precedence_collate_4(self):
        self.assert_compile(
            and_(
                (self.table1.c.name == literal('foo')).collate('utf-8'),
                (self.table2.c.field == literal('bar')).collate('utf-8'),
            ),
            "mytable.name = :param_1 COLLATE utf-8 "
            "AND op.field = :param_2 COLLATE utf-8"
        )

    def test_operator_precedence_collate_5(self):
        self.assert_compile(
            select([self.table1.c.name]).order_by(
                self.table1.c.name.collate('utf-8').desc()),
            "SELECT mytable.name FROM mytable "
            "ORDER BY mytable.name COLLATE utf-8 DESC"
        )

    def test_operator_precedence_collate_6(self):
        self.assert_compile(
            select([self.table1.c.name]).order_by(
                self.table1.c.name.collate('utf-8').desc().nullslast()),
            "SELECT mytable.name FROM mytable "
            "ORDER BY mytable.name COLLATE utf-8 DESC NULLS LAST"
        )

    def test_operator_precedence_collate_7(self):
        self.assert_compile(
            select([self.table1.c.name]).order_by(
                self.table1.c.name.collate('utf-8').asc()),
            "SELECT mytable.name FROM mytable "
            "ORDER BY mytable.name COLLATE utf-8 ASC"
        )

    def test_commutative_operators(self):
        self.assert_compile(
            literal("a") + literal("b") * literal("c"),
            ":param_1 || :param_2 * :param_3"
        )

    def test_op_operators(self):
        self.assert_compile(
            self.table1.select(self.table1.c.myid.op('hoho')(12) == 14),
            "SELECT mytable.myid, mytable.name, mytable.description FROM "
            "mytable WHERE (mytable.myid hoho :myid_1) = :param_1"
        )

    def test_op_operators_comma_precedence(self):
        self.assert_compile(
            func.foo(self.table1.c.myid.op('hoho')(12)),
            "foo(mytable.myid hoho :myid_1)"
        )

    def test_op_operators_comparison_precedence(self):
        self.assert_compile(
            self.table1.c.myid.op('hoho')(12) == 5,
            "(mytable.myid hoho :myid_1) = :param_1"
        )

    def test_op_operators_custom_precedence(self):
        op1 = self.table1.c.myid.op('hoho', precedence=5)
        op2 = op1(5).op('lala', precedence=4)(4)
        op3 = op1(5).op('lala', precedence=6)(4)

        self.assert_compile(op2, "mytable.myid hoho :myid_1 lala :param_1")
        self.assert_compile(op3, "(mytable.myid hoho :myid_1) lala :param_1")


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
        # - lower precedence than /
        self.assert_compile((f - f) / (f - f), "(f - f) / (f - f)")

    def test_associativity_18(self):
        f = column('f')
        # / higher precedence than -
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

    def test_associativity_22(self):
        f = column('f')
        self.assert_compile((f==f) == f, '(f = f) = f')

    def test_associativity_23(self):
        f = column('f')
        self.assert_compile((f!=f) != f, '(f != f) != f')


class IsDistinctFromTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
                   column('myid', Integer),
                   )

    def test_is_distinct_from(self):
        self.assert_compile(self.table1.c.myid.is_distinct_from(1),
                            "mytable.myid IS DISTINCT FROM :myid_1")

    def test_is_distinct_from_sqlite(self):
        self.assert_compile(self.table1.c.myid.is_distinct_from(1),
                            "mytable.myid IS NOT ?",
                            dialect=sqlite.dialect())

    def test_is_distinct_from_postgresql(self):
        self.assert_compile(self.table1.c.myid.is_distinct_from(1),
                            "mytable.myid IS DISTINCT FROM %(myid_1)s",
                            dialect=postgresql.dialect())

    def test_not_is_distinct_from(self):
        self.assert_compile(~self.table1.c.myid.is_distinct_from(1),
                            "mytable.myid IS NOT DISTINCT FROM :myid_1")

    def test_not_is_distinct_from_postgresql(self):
        self.assert_compile(~self.table1.c.myid.is_distinct_from(1),
                            "mytable.myid IS NOT DISTINCT FROM %(myid_1)s",
                            dialect=postgresql.dialect())

    def test_isnot_distinct_from(self):
        self.assert_compile(self.table1.c.myid.isnot_distinct_from(1),
                            "mytable.myid IS NOT DISTINCT FROM :myid_1")

    def test_isnot_distinct_from_sqlite(self):
        self.assert_compile(self.table1.c.myid.isnot_distinct_from(1),
                            "mytable.myid IS ?",
                            dialect=sqlite.dialect())

    def test_isnot_distinct_from_postgresql(self):
        self.assert_compile(self.table1.c.myid.isnot_distinct_from(1),
                            "mytable.myid IS NOT DISTINCT FROM %(myid_1)s",
                            dialect=postgresql.dialect())

    def test_not_isnot_distinct_from(self):
        self.assert_compile(~self.table1.c.myid.isnot_distinct_from(1),
                            "mytable.myid IS DISTINCT FROM :myid_1")

    def test_not_isnot_distinct_from_postgresql(self):
        self.assert_compile(~self.table1.c.myid.isnot_distinct_from(1),
                            "mytable.myid IS DISTINCT FROM %(myid_1)s",
                            dialect=postgresql.dialect())


class InTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
                   column('myid', Integer),
                   )
    table2 = table(
        'myothertable',
        column('otherid', Integer),
        column('othername', String)
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
        self.assert_compile(
            self.table1.c.myid.in_(
                [
                    literal('a') +
                    literal('a'),
                    literal('b')]),
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
        self.assert_compile(
            self.table1.c.myid.in_(
                [
                    literal('a'),
                    self.table1.c.myid +
                    'a']),
            "mytable.myid IN (:param_1, mytable.myid + :myid_1)")

    def test_in_18(self):
        self.assert_compile(
            self.table1.c.myid.in_(
                [
                    literal(1),
                    'a' +
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
        ), "mytable.myid IN ("
            "SELECT mytable.myid FROM mytable WHERE mytable.myid = :myid_1 "
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
            "SELECT mytable.myid, "
            "myothertable.otherid, myothertable.othername FROM mytable "
            "JOIN myothertable ON mytable.myid = myothertable.otherid "
            "WHERE myothertable.otherid IN (SELECT myothertable.otherid "
            "FROM myothertable ORDER BY myothertable.othername "
            "LIMIT :param_1) ORDER BY mytable.myid",
            {'param_1': 10}
        )

    def test_in_28(self):
        self.assert_compile(
            self.table1.c.myid.in_([None]),
            "mytable.myid IN (NULL)"
        )

    @testing.emits_warning('.*empty sequence.*')
    def test_in_29(self):
        self.assert_compile(self.table1.c.myid.notin_([]),
                            "mytable.myid = mytable.myid")

    @testing.emits_warning('.*empty sequence.*')
    def test_in_30(self):
        self.assert_compile(~self.table1.c.myid.in_([]),
                            "mytable.myid = mytable.myid")


class MathOperatorTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
                   column('myid', Integer),
                   )

    def _test_math_op(self, py_op, sql_op):
        for (lhs, rhs, res) in (
            (5, self.table1.c.myid, ':myid_1 %s mytable.myid'),
            (5, literal(5), ':param_1 %s :param_2'),
            (self.table1.c.myid, 'b', 'mytable.myid %s :myid_1'),
            (self.table1.c.myid, literal(2.7), 'mytable.myid %s :param_1'),
            (self.table1.c.myid, self.table1.c.myid,
             'mytable.myid %s mytable.myid'),
            (literal(5), 8, ':param_1 %s :param_2'),
            (literal(6), self.table1.c.myid, ':param_1 %s mytable.myid'),
            (literal(7), literal(5.5), ':param_1 %s :param_2'),
        ):
            self.assert_compile(py_op(lhs, rhs), res % sql_op)

    def test_math_op_add(self):
        self._test_math_op(operator.add, '+')

    def test_math_op_mul(self):
        self._test_math_op(operator.mul, '*')

    def test_math_op_sub(self):
        self._test_math_op(operator.sub, '-')

    def test_math_op_div(self):
        if util.py3k:
            self._test_math_op(operator.truediv, '/')
        else:
            self._test_math_op(operator.div, '/')

    def test_math_op_mod(self):
        self._test_math_op(operator.mod, '%')


class ComparisonOperatorTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
                   column('myid', Integer),
                   )

    def test_pickle_operators_one(self):
        clause = (self.table1.c.myid == 12) & \
            self.table1.c.myid.between(15, 20) & \
            self.table1.c.myid.like('hoho')
        eq_(str(clause), str(util.pickle.loads(util.pickle.dumps(clause))))

    def test_pickle_operators_two(self):
        clause = tuple_(1, 2, 3)
        eq_(str(clause), str(util.pickle.loads(util.pickle.dumps(clause))))

    def _test_comparison_op(self, py_op, fwd_op, rev_op):
        dt = datetime.datetime(2012, 5, 10, 15, 27, 18)
        for (lhs, rhs, l_sql, r_sql) in (
            ('a', self.table1.c.myid, ':myid_1', 'mytable.myid'),
            ('a', literal('b'), ':param_2', ':param_1'),  # note swap!
            (self.table1.c.myid, 'b', 'mytable.myid', ':myid_1'),
            (self.table1.c.myid, literal('b'), 'mytable.myid', ':param_1'),
            (self.table1.c.myid, self.table1.c.myid,
             'mytable.myid', 'mytable.myid'),
            (literal('a'), 'b', ':param_1', ':param_2'),
            (literal('a'), self.table1.c.myid, ':param_1', 'mytable.myid'),
            (literal('a'), literal('b'), ':param_1', ':param_2'),
            (dt, literal('b'), ':param_2', ':param_1'),
            (literal('b'), dt, ':param_1', ':param_2'),
        ):

            # the compiled clause should match either (e.g.):
            # 'a' < 'b' -or- 'b' > 'a'.
            compiled = str(py_op(lhs, rhs))
            fwd_sql = "%s %s %s" % (l_sql, fwd_op, r_sql)
            rev_sql = "%s %s %s" % (r_sql, rev_op, l_sql)

            self.assert_(compiled == fwd_sql or compiled == rev_sql,
                         "\n'" + compiled + "'\n does not match\n'" +
                         fwd_sql + "'\n or\n'" + rev_sql + "'")

    def test_comparison_operators_lt(self):
        self._test_comparison_op(operator.lt, '<', '>'),

    def test_comparison_operators_gt(self):
        self._test_comparison_op(operator.gt, '>', '<')

    def test_comparison_operators_eq(self):
        self._test_comparison_op(operator.eq, '=', '=')

    def test_comparison_operators_ne(self):
        self._test_comparison_op(operator.ne, '!=', '!=')

    def test_comparison_operators_le(self):
        self._test_comparison_op(operator.le, '<=', '>=')

    def test_comparison_operators_ge(self):
        self._test_comparison_op(operator.ge, '>=', '<=')


class NonZeroTest(fixtures.TestBase):

    def _raises(self, expr):
        assert_raises_message(
            TypeError,
            "Boolean value of this clause is not defined",
            bool, expr
        )

    def _assert_true(self, expr):
        is_(bool(expr), True)

    def _assert_false(self, expr):
        is_(bool(expr), False)

    def test_column_identity_eq(self):
        c1 = column('c1')
        self._assert_true(c1 == c1)

    def test_column_identity_gt(self):
        c1 = column('c1')
        self._raises(c1 > c1)

    def test_column_compare_eq(self):
        c1, c2 = column('c1'), column('c2')
        self._assert_false(c1 == c2)

    def test_column_compare_gt(self):
        c1, c2 = column('c1'), column('c2')
        self._raises(c1 > c2)

    def test_binary_identity_eq(self):
        c1 = column('c1')
        expr = c1 > 5
        self._assert_true(expr == expr)

    def test_labeled_binary_identity_eq(self):
        c1 = column('c1')
        expr = (c1 > 5).label(None)
        self._assert_true(expr == expr)

    def test_annotated_binary_identity_eq(self):
        c1 = column('c1')
        expr1 = (c1 > 5)
        expr2 = expr1._annotate({"foo": "bar"})
        self._assert_true(expr1 == expr2)

    def test_labeled_binary_compare_gt(self):
        c1 = column('c1')
        expr1 = (c1 > 5).label(None)
        expr2 = (c1 > 5).label(None)
        self._assert_false(expr1 == expr2)


class NegationTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
                   column('myid', Integer),
                   column('name', String),
                   )

    def test_negate_operators_1(self):
        for (py_op, op) in (
            (operator.neg, '-'),
            (operator.inv, 'NOT '),
        ):
            for expr, expected in (
                (self.table1.c.myid, "mytable.myid"),
                (literal("foo"), ":param_1"),
            ):
                self.assert_compile(py_op(expr), "%s%s" % (op, expected))

    def test_negate_operators_2(self):
        self.assert_compile(
            self.table1.select((self.table1.c.myid != 12) &
                               ~(self.table1.c.name == 'john')),
            "SELECT mytable.myid, mytable.name FROM "
            "mytable WHERE mytable.myid != :myid_1 "
            "AND mytable.name != :name_1"
        )

    def test_negate_operators_3(self):
        self.assert_compile(
            self.table1.select((self.table1.c.myid != 12) &
                               ~(self.table1.c.name.between('jack', 'john'))),
            "SELECT mytable.myid, mytable.name FROM "
            "mytable WHERE mytable.myid != :myid_1 AND "
            "mytable.name NOT BETWEEN :name_1 AND :name_2"
        )

    def test_negate_operators_4(self):
        self.assert_compile(
            self.table1.select((self.table1.c.myid != 12) &
                               ~and_(self.table1.c.name == 'john',
                                     self.table1.c.name == 'ed',
                                     self.table1.c.name == 'fred')),
            "SELECT mytable.myid, mytable.name FROM "
            "mytable WHERE mytable.myid != :myid_1 AND "
            "NOT (mytable.name = :name_1 AND mytable.name = :name_2 "
            "AND mytable.name = :name_3)"
        )

    def test_negate_operators_5(self):
        self.assert_compile(
            self.table1.select(
                (self.table1.c.myid != 12) & ~self.table1.c.name),
            "SELECT mytable.myid, mytable.name FROM "
            "mytable WHERE mytable.myid != :myid_1 AND NOT mytable.name")

    def test_negate_operator_type(self):
        is_(
            (-self.table1.c.myid).type,
            self.table1.c.myid.type,
        )


class LikeTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
                   column('myid', Integer),
                   column('name', String),
                   )

    def test_like_1(self):
        self.assert_compile(
            self.table1.c.myid.like('somstr'),
            "mytable.myid LIKE :myid_1")

    def test_like_2(self):
        self.assert_compile(
            ~self.table1.c.myid.like('somstr'),
            "mytable.myid NOT LIKE :myid_1")

    def test_like_3(self):
        self.assert_compile(
            self.table1.c.myid.like('somstr', escape='\\'),
            "mytable.myid LIKE :myid_1 ESCAPE '\\'")

    def test_like_4(self):
        self.assert_compile(
            ~self.table1.c.myid.like('somstr', escape='\\'),
            "mytable.myid NOT LIKE :myid_1 ESCAPE '\\'")

    def test_like_5(self):
        self.assert_compile(
            self.table1.c.myid.ilike('somstr', escape='\\'),
            "lower(mytable.myid) LIKE lower(:myid_1) ESCAPE '\\'")

    def test_like_6(self):
        self.assert_compile(
            ~self.table1.c.myid.ilike('somstr', escape='\\'),
            "lower(mytable.myid) NOT LIKE lower(:myid_1) ESCAPE '\\'")

    def test_like_7(self):
        self.assert_compile(
            self.table1.c.myid.ilike('somstr', escape='\\'),
            "mytable.myid ILIKE %(myid_1)s ESCAPE '\\\\'",
            dialect=postgresql.dialect())

    def test_like_8(self):
        self.assert_compile(
            ~self.table1.c.myid.ilike('somstr', escape='\\'),
            "mytable.myid NOT ILIKE %(myid_1)s ESCAPE '\\\\'",
            dialect=postgresql.dialect())

    def test_like_9(self):
        self.assert_compile(
            self.table1.c.name.ilike('%something%'),
            "lower(mytable.name) LIKE lower(:name_1)")

    def test_like_10(self):
        self.assert_compile(
            self.table1.c.name.ilike('%something%'),
            "mytable.name ILIKE %(name_1)s",
            dialect=postgresql.dialect())

    def test_like_11(self):
        self.assert_compile(
            ~self.table1.c.name.ilike('%something%'),
            "lower(mytable.name) NOT LIKE lower(:name_1)")

    def test_like_12(self):
        self.assert_compile(
            ~self.table1.c.name.ilike('%something%'),
            "mytable.name NOT ILIKE %(name_1)s",
            dialect=postgresql.dialect())


class BetweenTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
                   column('myid', Integer),
                   column('name', String),
                   )

    def test_between_1(self):
        self.assert_compile(
            self.table1.c.myid.between(1, 2),
            "mytable.myid BETWEEN :myid_1 AND :myid_2")

    def test_between_2(self):
        self.assert_compile(
            ~self.table1.c.myid.between(1, 2),
            "mytable.myid NOT BETWEEN :myid_1 AND :myid_2")

    def test_between_3(self):
        self.assert_compile(
            self.table1.c.myid.between(1, 2, symmetric=True),
            "mytable.myid BETWEEN SYMMETRIC :myid_1 AND :myid_2")

    def test_between_4(self):
        self.assert_compile(
            ~self.table1.c.myid.between(1, 2, symmetric=True),
            "mytable.myid NOT BETWEEN SYMMETRIC :myid_1 AND :myid_2")

    def test_between_5(self):
        self.assert_compile(
            between(self.table1.c.myid, 1, 2, symmetric=True),
            "mytable.myid BETWEEN SYMMETRIC :myid_1 AND :myid_2")

    def test_between_6(self):
        self.assert_compile(
            ~between(self.table1.c.myid, 1, 2, symmetric=True),
            "mytable.myid NOT BETWEEN SYMMETRIC :myid_1 AND :myid_2")


class MatchTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    table1 = table('mytable',
                   column('myid', Integer),
                   column('name', String),
                   )

    def test_match_1(self):
        self.assert_compile(self.table1.c.myid.match('somstr'),
                            "mytable.myid MATCH ?",
                            dialect=sqlite.dialect())

    def test_match_2(self):
        self.assert_compile(
            self.table1.c.myid.match('somstr'),
            "MATCH (mytable.myid) AGAINST (%s IN BOOLEAN MODE)",
            dialect=mysql.dialect())

    def test_match_3(self):
        self.assert_compile(self.table1.c.myid.match('somstr'),
                            "CONTAINS (mytable.myid, :myid_1)",
                            dialect=mssql.dialect())

    def test_match_4(self):
        self.assert_compile(self.table1.c.myid.match('somstr'),
                            "mytable.myid @@ to_tsquery(%(myid_1)s)",
                            dialect=postgresql.dialect())

    def test_match_5(self):
        self.assert_compile(self.table1.c.myid.match('somstr'),
                            "CONTAINS (mytable.myid, :myid_1)",
                            dialect=oracle.dialect())

    def test_match_is_now_matchtype(self):
        expr = self.table1.c.myid.match('somstr')
        assert expr.type._type_affinity is MatchType()._type_affinity
        assert isinstance(expr.type, MatchType)

    def test_boolean_inversion_postgresql(self):
        self.assert_compile(
            ~self.table1.c.myid.match('somstr'),
            "NOT mytable.myid @@ to_tsquery(%(myid_1)s)",
            dialect=postgresql.dialect())

    def test_boolean_inversion_mysql(self):
        # because mysql doesnt have native boolean
        self.assert_compile(
            ~self.table1.c.myid.match('somstr'),
            "NOT MATCH (mytable.myid) AGAINST (%s IN BOOLEAN MODE)",
            dialect=mysql.dialect())

    def test_boolean_inversion_mssql(self):
        # because mssql doesnt have native boolean
        self.assert_compile(
            ~self.table1.c.myid.match('somstr'),
            "NOT CONTAINS (mytable.myid, :myid_1)",
            dialect=mssql.dialect())


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


class CustomOpTest(fixtures.TestBase):

    def test_is_comparison(self):
        c = column('x')
        c2 = column('y')
        op1 = c.op('$', is_comparison=True)(c2).operator
        op2 = c.op('$', is_comparison=False)(c2).operator

        assert operators.is_comparison(op1)
        assert not operators.is_comparison(op2)


class TupleTypingTest(fixtures.TestBase):

    def _assert_types(self, expr):
        eq_(expr.clauses[0].type._type_affinity, Integer)
        eq_(expr.clauses[1].type._type_affinity, String)
        eq_(expr.clauses[2].type._type_affinity, LargeBinary()._type_affinity)

    def test_type_coersion_on_eq(self):
        a, b, c = column(
            'a', Integer), column(
            'b', String), column(
            'c', LargeBinary)
        t1 = tuple_(a, b, c)
        expr = t1 == (3, 'hi', 'there')
        self._assert_types(expr.right)

    def test_type_coersion_on_in(self):
        a, b, c = column(
            'a', Integer), column(
            'b', String), column(
            'c', LargeBinary)
        t1 = tuple_(a, b, c)
        expr = t1.in_([(3, 'hi', 'there'), (4, 'Q', 'P')])
        eq_(len(expr.right.clauses), 2)
        for elem in expr.right.clauses:
            self._assert_types(elem)


class AnyAllTest(fixtures.TestBase, testing.AssertsCompiledSQL):
    __dialect__ = 'default'

    def _fixture(self):
        m = MetaData()

        t = Table(
            'tab1', m,
            Column('arrval', ARRAY(Integer)),
            Column('data', Integer)
        )
        return t

    def test_any_array(self):
        t = self._fixture()

        self.assert_compile(
            5 == any_(t.c.arrval),
            ":param_1 = ANY (tab1.arrval)",
            checkparams={"param_1": 5}
        )

    def test_all_array(self):
        t = self._fixture()

        self.assert_compile(
            5 == all_(t.c.arrval),
            ":param_1 = ALL (tab1.arrval)",
            checkparams={"param_1": 5}
        )

    def test_any_comparator_array(self):
        t = self._fixture()

        self.assert_compile(
            5 > any_(t.c.arrval),
            ":param_1 > ANY (tab1.arrval)",
            checkparams={"param_1": 5}
        )

    def test_all_comparator_array(self):
        t = self._fixture()

        self.assert_compile(
            5 > all_(t.c.arrval),
            ":param_1 > ALL (tab1.arrval)",
            checkparams={"param_1": 5}
        )

    def test_any_comparator_array_wexpr(self):
        t = self._fixture()

        self.assert_compile(
            t.c.data > any_(t.c.arrval),
            "tab1.data > ANY (tab1.arrval)",
            checkparams={}
        )

    def test_all_comparator_array_wexpr(self):
        t = self._fixture()

        self.assert_compile(
            t.c.data > all_(t.c.arrval),
            "tab1.data > ALL (tab1.arrval)",
            checkparams={}
        )

    def test_illegal_ops(self):
        t = self._fixture()

        assert_raises_message(
            exc.ArgumentError,
            "Only comparison operators may be used with ANY/ALL",
            lambda: 5 + all_(t.c.arrval)
        )

        # TODO:
        # this is invalid but doesn't raise an error,
        # as the left-hand side just does its thing.  Types
        # would need to reject their right-hand side.
        self.assert_compile(
            t.c.data + all_(t.c.arrval),
            "tab1.data + ALL (tab1.arrval)"
        )

    def test_any_array_comparator_accessor(self):
        t = self._fixture()

        self.assert_compile(
            t.c.arrval.any(5, operator.gt),
            ":param_1 > ANY (tab1.arrval)",
            checkparams={"param_1": 5}
        )

    def test_all_array_comparator_accessor(self):
        t = self._fixture()

        self.assert_compile(
            t.c.arrval.all(5, operator.gt),
            ":param_1 > ALL (tab1.arrval)",
            checkparams={"param_1": 5}
        )

    def test_any_array_expression(self):
        t = self._fixture()

        self.assert_compile(
            5 == any_(t.c.arrval[5:6] + postgresql.array([3, 4])),
            "%(param_1)s = ANY (tab1.arrval[%(arrval_1)s:%(arrval_2)s] || "
            "ARRAY[%(param_2)s, %(param_3)s])",
            checkparams={
                'arrval_2': 6, 'param_1': 5, 'param_3': 4,
                'arrval_1': 5, 'param_2': 3},
            dialect='postgresql'
        )

    def test_all_array_expression(self):
        t = self._fixture()

        self.assert_compile(
            5 == all_(t.c.arrval[5:6] + postgresql.array([3, 4])),
            "%(param_1)s = ALL (tab1.arrval[%(arrval_1)s:%(arrval_2)s] || "
            "ARRAY[%(param_2)s, %(param_3)s])",
            checkparams={
                'arrval_2': 6, 'param_1': 5, 'param_3': 4,
                'arrval_1': 5, 'param_2': 3},
            dialect='postgresql'
        )

    def test_any_subq(self):
        t = self._fixture()

        self.assert_compile(
            5 == any_(select([t.c.data]).where(t.c.data < 10)),
            ":param_1 = ANY (SELECT tab1.data "
            "FROM tab1 WHERE tab1.data < :data_1)",
            checkparams={'data_1': 10, 'param_1': 5}
        )

    def test_all_subq(self):
        t = self._fixture()

        self.assert_compile(
            5 == all_(select([t.c.data]).where(t.c.data < 10)),
            ":param_1 = ALL (SELECT tab1.data "
            "FROM tab1 WHERE tab1.data < :data_1)",
            checkparams={'data_1': 10, 'param_1': 5}
        )

