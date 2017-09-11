"""Evaluating SQL expressions on ORM objects"""

from sqlalchemy import String, Integer, bindparam
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing import fixtures

from sqlalchemy import and_, or_, not_
from sqlalchemy.orm import evaluator
from sqlalchemy.orm import mapper

from sqlalchemy.orm import relationship, Session
from sqlalchemy import ForeignKey
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import is_
from sqlalchemy import inspect
from sqlalchemy.testing import expect_warnings

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
              Column('name', String(64)),
              Column('othername', String(64)))

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

        eval_eq(User.name == None,  # noqa
                testcases=[(User(name='foo'), False), (User(name=None), True)])

    def test_warn_on_unannotated_matched_column(self):
        User = self.classes.User

        compiler = evaluator.EvaluatorCompiler(User)

        with expect_warnings(
            r"Evaluating non-mapped column expression 'othername' "
                "onto ORM instances; this is a deprecated use case."):
            meth = compiler.process(User.name == Column('othername', String))

        u1 = User(id=5)
        meth(u1)

    def test_raise_on_unannotated_unmatched_column(self):
        User = self.classes.User

        compiler = evaluator.EvaluatorCompiler(User)

        assert_raises_message(
            evaluator.UnevaluatableError,
            "Cannot evaluate column: foo",
            compiler.process, User.id == Column('foo', Integer)
        )

        # if we let the above method through as we did
        # prior to [ticket:3366], we would get
        # AttributeError: 'User' object has no attribute 'foo'
        # u1 = User(id=5)
        # meth(u1)

    def test_true_false(self):
        User = self.classes.User

        eval_eq(
            User.name == False,  # noqa
            testcases=[
                (User(name='foo'), False),
                (User(name=True), False),
                (User(name=False), True),
            ]
        )

        eval_eq(
            User.name == True,  # noqa
            testcases=[
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


class M2OEvaluateTest(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Parent(Base):
            __tablename__ = "parent"
            id = Column(Integer, primary_key=True)

        class Child(Base):
            __tablename__ = "child"
            _id_parent = Column(
                "id_parent", Integer, ForeignKey(Parent.id), primary_key=True)
            name = Column(String(50), primary_key=True)
            parent = relationship(Parent)

    def test_delete(self):
        Parent, Child = self.classes('Parent', 'Child')

        session = Session()

        p = Parent(id=1)
        session.add(p)
        session.commit()

        c = Child(name="foo", parent=p)
        session.add(c)
        session.commit()

        session.query(Child).filter(Child.parent == p).delete("evaluate")

        is_(
            inspect(c).deleted, True
        )
