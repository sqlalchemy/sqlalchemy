from test.lib import fixtures, testing
from test.lib.testing import assert_raises_message
from sqlalchemy.sql import column, desc, asc, literal, collate
from sqlalchemy.sql.expression import _BinaryExpression as BinaryExpression, \
                ClauseList, _Grouping as Grouping, \
                _UnaryExpression as UnaryExpression
from sqlalchemy.sql import operators
from sqlalchemy import exc
from sqlalchemy.schema import Column, Table, MetaData
from sqlalchemy.types import Integer, TypeEngine, TypeDecorator, UserDefinedType
from sqlalchemy.dialects import mysql, firebird

from sqlalchemy import text, literal_column

class DefaultColumnComparatorTest(fixtures.TestBase):

    def _do_scalar_test(self, operator, compare_to):
        left = column('left')

        assert operator(left).compare(
            compare_to(left)
        )

    def _do_operate_test(self, operator, right=column('right')):
        left = column('left')

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


    def test_collate(self):
        left = column('left')
        right = "some collation"
        operators.collate(left, right).compare(
            collate(left, right)
        )


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
            "NOT (x LIKE '%%' || :x_1 || '%%')",
            checkparams={'x_1': 'y'}
        )

    def test_not_contains_escape(self):
        self.assert_compile(
            ~column('x').contains('y', escape='\\'),
            "NOT (x LIKE '%%' || :x_1 || '%%' ESCAPE '\\')",
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
            "NOT (x LIKE concat(concat('%%', %s), '%%'))",
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
            "NOT (x LIKE :x_1 || '%%')",
            checkparams={'x_1': 'y'}
        )

    def test_not_startswith_escape(self):
        self.assert_compile(
            ~column('x').startswith('y', escape='\\'),
            "NOT (x LIKE :x_1 || '%%' ESCAPE '\\')",
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
            "NOT (x LIKE concat(%s, '%%'))",
            checkparams={'x_1': 'y'},
            dialect=mysql.dialect()
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
            "NOT (x LIKE '%%' || :x_1)",
            checkparams={'x_1': 'y'}
        )

    def test_not_endswith_escape(self):
        self.assert_compile(
            ~column('x').endswith('y', escape='\\'),
            "NOT (x LIKE '%%' || :x_1 ESCAPE '\\')",
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
            "NOT (x LIKE concat('%%', %s))",
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

