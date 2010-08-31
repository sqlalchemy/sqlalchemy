from functools import update_wrapper
import new

class method(object):
    def __init__(self, func, expr=None):
        self.func = func
        self.expr = expr or func
        
    def __get__(self, instance, owner):
        if instance is None:
            return new.instancemethod(self.expr, owner, owner.__class__)
        else:
            return new.instancemethod(self.func, instance, owner)

    def expression(self, expr):
        self.expr = expr
        return self

class property_(object):
    def __init__(self, fget, fset=None, fdel=None, expr=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        self.expr = expr or fget
        update_wrapper(self, fget)

    def __get__(self, instance, owner):
        if instance is None:
            return self.expr(owner)
        else:
            return self.fget(instance)
            
    def __set__(self, instance, value):
        self.fset(instance, value)
        
    def __delete__(self, instance):
        self.fdel(instance)
    
    def setter(self, fset):
        self.fset = fset
        return self

    def deleter(self, fdel):
        self.fdel = fdel
        return self
    
    def expression(self, expr):
        self.expr = expr
        return self

### Example code

from sqlalchemy import Table, Column, Integer, create_engine, func
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class BaseInterval(object):
    @method
    def contains(self,point):
        """Return true if the interval contains the given interval."""
        
        return (self.start <= point) & (point < self.end)
    
    @method
    def intersects(self, other):
        """Return true if the interval intersects the given interval."""
        
        return (self.start < other.end) & (self.end > other.start)
    
    @method
    def _max(self, x, y):
        """Return the max of two values."""
        
        return max(x, y)
    
    @_max.expression
    def _max(cls, x, y):
        """Return the SQL max of two values."""
        
        return func.max(x, y)
        
    @method
    def max_length(self, other):
        """Return the longer length of this interval and another."""
        
        return self._max(self.length, other.length)
    
    def __repr__(self):
        return "%s(%s..%s)" % (self.__class__.__name__, self.start, self.end)
    
class Interval1(BaseInterval, Base):
    """Interval stored as endpoints"""
    
    __table__ = Table('interval1', Base.metadata,
                Column('id', Integer, primary_key=True),
                Column('start', Integer, nullable=False),
                Column('end', Integer, nullable=False)
            )

    def __init__(self, start, end):
        self.start = start
        self.end = end

    @property_
    def length(self):
        return self.end - self.start

class Interval2(BaseInterval, Base):
    """Interval stored as start and length"""
    
    __table__ = Table('interval2', Base.metadata,
                Column('id', Integer, primary_key=True),
                Column('start', Integer, nullable=False),
                Column('length', Integer, nullable=False)
            )

    def __init__(self, start, length):
        self.start = start
        self.length = length
    
    @property_
    def end(self):
        return self.start + self.length

    

engine = create_engine('sqlite://', echo=True)

Base.metadata.create_all(engine)

session = sessionmaker(engine)()

intervals = [Interval1(1,4), Interval1(3,15), Interval1(11,16)]

for interval in intervals:
    session.add(interval)
    session.add(Interval2(interval.start, interval.length))

session.commit()

for Interval in (Interval1, Interval2):
    print "Querying using interval class %s" % Interval.__name__
    
    print
    print '-- length less than 10'
    print [(i, i.length) for i in 
                session.query(Interval).filter(Interval.length < 10).all()]
    
    print
    print '-- contains 12'
    print session.query(Interval).filter(Interval.contains(12)).all()
    
    print
    print '-- intersects 2..10'
    other = Interval1(2,10)
    result = session.query(Interval).\
                    filter(Interval.intersects(other)).\
                    order_by(Interval.length).all()
    print [(interval, interval.intersects(other)) for interval in result]
    
    print
    print '-- longer length'
    interval_alias = aliased(Interval)
    print session.query(Interval.length, 
                            interval_alias.length,
                            Interval.max_length(interval_alias)).all()
