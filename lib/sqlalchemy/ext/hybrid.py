"""Define attributes on ORM-mapped classes that have 'hybrid' behavior.

'hybrid' means the attribute has distinct behaviors defined at the
class level and at the instance level.

Consider a table `interval` as below::

    from sqlalchemy import MetaData, Table, Column, Integer
    from sqlalchemy.orm import mapper, create_session
    
    engine = create_engine('sqlite://')
    metadata = MetaData()

    interval_table = Table('interval', metadata,
        Column('id', Integer, primary_key=True),
        Column('start', Integer, nullable=False),
        Column('end', Integer, nullable=False))
    metadata.create_all(engine)
    
We can define higher level functions on mapped classes that produce SQL
expressions at the class level, and Python expression evaluation at the
instance level.  Below, each function decorated with :func:`hybrid.method`
or :func:`hybrid.property` may receive ``self`` as an instance of the class,
or as the class itself::
    
    # A base class for intervals

    from sqlalchemy.orm import hybrid
    
    class Interval(object):
        def __init__(self, start, end):
            self.start = start
            self.end = end
        
        @hybrid.property
        def length(self):
            return self.end - self.start

        @hybrid.method
        def contains(self,point):
            return (self.start <= point) & (point < self.end)
    
        @hybrid.method
        def intersects(self, other):
            return (self.start < other.end) & (self.end > other.start)

    mapper(Interval1, interval_table1)

    session = sessionmaker(engine)()

    session.add_all(
        [Interval1(1,4), Interval1(3,15), Interval1(11,16)]
    )
    intervals = 

    for interval in intervals:
        session.add(interval)
        session.add(Interval2(interval.start, interval.length))

    session.commit()

    ### TODO ADD EXAMPLES HERE AND STUFF THIS ISN'T FINISHED ###
    
"""
from sqlalchemy import util

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
        util.update_wrapper(self, fget)

    def __get__(self, instance, owner):
        if instance is None:
            return util.update_wrapper(self.expr(owner), self)
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
    

