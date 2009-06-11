"""Evluating SQL expressions on ORM objects"""
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy import String, Integer, select
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
from sqlalchemy.orm import mapper, create_session
from sqlalchemy.test.testing import eq_
from test.orm import _base

from sqlalchemy import and_, or_, not_
from sqlalchemy.orm import evaluator

compiler = evaluator.EvaluatorCompiler()
def eval_eq(clause, testcases=None):
    evaluator = compiler.process(clause)
    def testeval(obj=None, expected_result=None):
        assert evaluator(obj) == expected_result, "%s != %r for %s with %r" % (evaluator(obj), expected_result, clause, obj)
    if testcases:
        for an_obj,result in testcases:
            testeval(an_obj, result)
    return testeval

class EvaluateTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(64)))
    
    @classmethod
    def setup_classes(cls):
        class User(_base.ComparableEntity):
            pass
    
    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(User, users)
    
    @testing.resolve_artifact_names
    def test_compare_to_value(self):
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
    
    @testing.resolve_artifact_names
    def test_compare_to_none(self):
        eval_eq(User.name == None, testcases=[
            (User(name='foo'), False),
            (User(name=None), True),
        ])
   
    @testing.resolve_artifact_names
    def test_boolean_ops(self):
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

    @testing.resolve_artifact_names
    def test_null_propagation(self):
        eval_eq((User.name == 'foo') == (User.id == 1), testcases=[
            (User(id=1, name='foo'), True),
            (User(id=2, name='foo'), False),
            (User(id=1, name='bar'), False),
            (User(id=2, name='bar'), True),
            (User(id=None, name='foo'), None),
            (User(id=None, name=None), None),
        ])

