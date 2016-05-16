"""Evaluating SQL expressions on ORM objects"""

from sqlalchemy import String, Integer, bindparam
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing import fixtures

from sqlalchemy import and_, or_, not_
from sqlalchemy.orm import evaluator
from sqlalchemy.orm import mapper

compiler = evaluator.EvaluatorCompiler()


def eval_eq(clause, testcases=None):
    evaluator = compiler.process(clause)

    def testeval(obj=None, expected_result=None):
        assert evaluator(obj) == expected_result, \
            "%s != %r for %s with %r" % (
                evaluator(obj), expected_result, clause, obj)
    if testcases:
        for an_obj, result in testcases:
            testeval(an_obj, result)
    return testeval


class EvaluateTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(64)))

    @classmethod
    def setup_classes(cls):
        class User(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        users, User = cls.tables.users, cls.classes.User

        mapper(User, users)

    def test_compare_to_value(self):
        User = self.classes.User

        eval_eq(User.name == 'foo', testcases=[
            (User(name='foo'), True),
            (User(name='bar'), False),
            (User(name=None), None),
        ])

        eval_eq(User.id < 5, testcases=[
            (User(id=3), True),
            (User(id=5), False),
            (User(id=None), None),
        ])

    def test_compare_to_callable_bind(self):
        User = self.classes.User

        eval_eq(
            User.name == bindparam('x', callable_=lambda: 'foo'),
            testcases=[
                (User(name='foo'), True),
                (User(name='bar'), False),
                (User(name=None), None),
            ]
        )

    def test_compare_to_none(self):
        User = self.classes.User

        eval_eq(User.name == None, testcases=[
            (User(name='foo'), False),
            (User(name=None), True),
        ])

    def test_true_false(self):
        User = self.classes.User

        eval_eq(
            User.name == False, testcases=[
                (User(name='foo'), False),
                (User(name=True), False),
                (User(name=False), True),
            ]
        )

        eval_eq(
            User.name == True, testcases=[
                (User(name='foo'), False),
                (User(name=True), True),
                (User(name=False), False),
            ]
        )

    def test_boolean_ops(self):
        User = self.classes.User

        eval_eq(and_(User.name == 'foo', User.id == 1), testcases=[
            (User(id=1, name='foo'), True),
            (User(id=2, name='foo'), False),
            (User(id=1, name='bar'), False),
            (User(id=2, name='bar'), False),
            (User(id=1, name=None), None),
        ])

        eval_eq(or_(User.name == 'foo', User.id == 1), testcases=[
            (User(id=1, name='foo'), True),
            (User(id=2, name='foo'), True),
            (User(id=1, name='bar'), True),
            (User(id=2, name='bar'), False),
            (User(id=1, name=None), True),
            (User(id=2, name=None), None),
        ])

        eval_eq(not_(User.id == 1), testcases=[
            (User(id=1), False),
            (User(id=2), True),
            (User(id=None), None),
        ])

    def test_null_propagation(self):
        User = self.classes.User

        eval_eq((User.name == 'foo') == (User.id == 1), testcases=[
            (User(id=1, name='foo'), True),
            (User(id=2, name='foo'), False),
            (User(id=1, name='bar'), False),
            (User(id=2, name='bar'), True),
            (User(id=None, name='foo'), None),
            (User(id=None, name=None), None),
        ])

