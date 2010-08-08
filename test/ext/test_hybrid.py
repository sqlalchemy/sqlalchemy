"""

tests for sqlalchemy.ext.hybrid TODO


"""


from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext import hybrid
from sqlalchemy.orm.interfaces import PropComparator


"""
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext import hybrid

Base = declarative_base()


class UCComparator(hybrid.Comparator):
    
    def __eq__(self, other):
        if other is None:
            return self.expression == None
        else:
            return func.upper(self.expression) == func.upper(other)

class A(Base):
    __tablename__ = 'a'
    id = Column(Integer, primary_key=True)
    _value = Column("value", String)

    @hybrid.property_
    def value(self):
        return int(self._value)

    @value.comparator
    def value(cls):
        return UCComparator(cls._value)
        
    @value.setter
    def value(self, v):
        self.value = v
print aliased(A).value
print aliased(A).__tablename__

sess = create_session()

print A.value == "foo"
print sess.query(A.value)
print sess.query(aliased(A).value)
print sess.query(aliased(A)).filter_by(value="foo")
"""

"""
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext import hybrid

Base = declarative_base()

class A(Base):
    __tablename__ = 'a'
    id = Column(Integer, primary_key=True)
    _value = Column("value", String)

    @hybrid.property
    def value(self):
        return int(self._value)
    
    @value.expression
    def value(cls):
        return func.foo(cls._value) + cls.bar_value

    @value.setter
    def value(self, v):
        self.value = v

    @hybrid.property
    def bar_value(cls):
        return func.bar(cls._value)
        
#print A.value
#print A.value.__doc__

print aliased(A).value
print aliased(A).__tablename__

sess = create_session()

print sess.query(A).filter_by(value="foo")

print sess.query(aliased(A)).filter_by(value="foo")


"""