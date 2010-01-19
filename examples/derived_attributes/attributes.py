
import new

class MethodDescriptor(object):
    def __init__(self, func):
        self.func = func
    def __get__(self, instance, owner):
        if instance is None:
            return new.instancemethod(self.func, owner, owner.__class__)
        else:
            return new.instancemethod(self.func, instance, owner)

class PropertyDescriptor(object):
    def __init__(self, fget, fset, fdel):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
    def __get__(self, instance, owner):
        if instance is None:
            return self.fget(owner)
        else:
            return self.fget(instance)
    def __set__(self, instance, value):
        self.fset(instance, value)
    def __delete__(self, instance):
        self.fdel(instance)
        
def hybrid(func):
    return MethodDescriptor(func)

def hybrid_property(fget, fset=None, fdel=None):
    return PropertyDescriptor(fget, fset, fdel)

### Example code

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.orm import mapper, create_session

metadata = MetaData('sqlite://')
metadata.bind.echo = True

print "Set up database metadata"

interval_table1 = Table('interval1', metadata,
    Column('id', Integer, primary_key=True),
    Column('start', Integer, nullable=False),
    Column('end', Integer, nullable=False))

interval_table2 = Table('interval2', metadata,
    Column('id', Integer, primary_key=True),
    Column('start', Integer, nullable=False),
    Column('length', Integer, nullable=False))

metadata.create_all()

# A base class for intervals

class BaseInterval(object):
    @hybrid
    def contains(self,point):
        return (self.start <= point) & (point < self.end)
    
    @hybrid
    def intersects(self, other):
        return (self.start < other.end) & (self.end > other.start)

    def __repr__(self):
        return "%s(%s..%s)" % (self.__class__.__name__, self.start, self.end)

# Interval stored as endpoints

class Interval1(BaseInterval):
    def __init__(self, start, end):
        self.start = start
        self.end = end
    
    length = hybrid_property(lambda s: s.end - s.start)

mapper(Interval1, interval_table1)

# Interval stored as start and length

class Interval2(BaseInterval):
    def __init__(self, start, length):
        self.start = start
        self.length = length
    
    end = hybrid_property(lambda s: s.start + s.length)

mapper(Interval2, interval_table2)

print "Create the data"

session = create_session()

intervals = [Interval1(1,4), Interval1(3,15), Interval1(11,16)]

for interval in intervals:
    session.add(interval)
    session.add(Interval2(interval.start, interval.length))

session.flush()

print "Clear the cache and do some queries"

session.expunge_all()

for Interval in (Interval1, Interval2):
    print "Querying using interval class %s" % Interval.__name__
    
    print
    print '-- length less than 10'
    print [(i, i.length) for i in session.query(Interval).filter(Interval.length < 10).all()]
    
    print
    print '-- contains 12'
    print session.query(Interval).filter(Interval.contains(12)).all()
    
    print
    print '-- intersects 2..10'
    other = Interval1(2,10)
    result = session.query(Interval).filter(Interval.intersects(other)).order_by(Interval.length).all()
    print [(interval, interval.intersects(other)) for interval in result]

