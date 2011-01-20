# ext/hybrid.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define attributes on ORM-mapped classes that have "hybrid" behavior.

"hybrid" means the attribute has distinct behaviors defined at the
class level and at the instance level.

The :mod:`~sqlalchemy.ext.hybrid` extension provides a special form of method
decorator, is around 50 lines of code and has almost no dependencies on the rest 
of SQLAlchemy.  It can in theory work with any class-level expression generator.

Consider a table ``interval`` as below::

    from sqlalchemy import MetaData, Table, Column, Integer

    metadata = MetaData()

    interval_table = Table('interval', metadata,
        Column('id', Integer, primary_key=True),
        Column('start', Integer, nullable=False),
        Column('end', Integer, nullable=False)
    )

We can define higher level functions on mapped classes that produce SQL
expressions at the class level, and Python expression evaluation at the
instance level.  Below, each function decorated with :func:`.hybrid_method`
or :func:`.hybrid_property` may receive ``self`` as an instance of the class,
or as the class itself::

    from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
    from sqlalchemy.orm import mapper, Session, aliased

    class Interval(object):
        def __init__(self, start, end):
            self.start = start
            self.end = end

        @hybrid_property
        def length(self):
            return self.end - self.start

        @hybrid_method
        def contains(self,point):
            return (self.start <= point) & (point < self.end)

        @hybrid_method
        def intersects(self, other):
            return self.contains(other.start) | self.contains(other.end)
    
    mapper(Interval, interval_table)

Above, the ``length`` property returns the difference between the ``end`` and
``start`` attributes.  With an instance of ``Interval``, this subtraction occurs
in Python, using normal Python descriptor mechanics::

    >>> i1 = Interval(5, 10)
    >>> i1.length
    5
    
At the class level, the usual descriptor behavior of returning the descriptor
itself is modified by :class:`.hybrid_property`, to instead evaluate the function 
body given the ``Interval`` class as the argument::
    
    >>> print Interval.length
    interval."end" - interval.start
    
    >>> print Session().query(Interval).filter(Interval.length > 10)
    SELECT interval.id AS interval_id, interval.start AS interval_start, 
    interval."end" AS interval_end 
    FROM interval 
    WHERE interval."end" - interval.start > :param_1
    
ORM methods such as :meth:`~.Query.filter_by` generally use ``getattr()`` to 
locate attributes, so can also be used with hybrid attributes::

    >>> print Session().query(Interval).filter_by(length=5)
    SELECT interval.id AS interval_id, interval.start AS interval_start, 
    interval."end" AS interval_end 
    FROM interval 
    WHERE interval."end" - interval.start = :param_1

The ``contains()`` and ``intersects()`` methods are decorated with :class:`.hybrid_method`.
This decorator applies the same idea to methods which accept
zero or more arguments.   The above methods return boolean values, and take advantage 
of the Python ``|`` and ``&`` bitwise operators to produce equivalent instance-level and 
SQL expression-level boolean behavior::

    >>> i1.contains(6)
    True
    >>> i1.contains(15)
    False
    >>> i1.intersects(Interval(7, 18))
    True
    >>> i1.intersects(Interval(25, 29))
    False
    
    >>> print Session().query(Interval).filter(Interval.contains(15))
    SELECT interval.id AS interval_id, interval.start AS interval_start, 
    interval."end" AS interval_end 
    FROM interval 
    WHERE interval.start <= :start_1 AND interval."end" > :end_1

    >>>  ia = aliased(Interval)
    >>> print Session().query(Interval, ia).filter(Interval.intersects(ia))
    SELECT interval.id AS interval_id, interval.start AS interval_start, 
    interval."end" AS interval_end, interval_1.id AS interval_1_id, 
    interval_1.start AS interval_1_start, interval_1."end" AS interval_1_end 
    FROM interval, interval AS interval_1 
    WHERE interval.start <= interval_1.start 
        AND interval."end" > interval_1.start 
        OR interval.start <= interval_1."end" 
        AND interval."end" > interval_1."end"
    
Defining Expression Behavior Distinct from Attribute Behavior
--------------------------------------------------------------

Our usage of the ``&`` and ``|`` bitwise operators above was fortunate, considering
our functions operated on two boolean values to return a new one.   In many cases, the construction
of an in-Python function and a SQLAlchemy SQL expression have enough differences that two
separate Python expressions should be defined.  The :mod:`~sqlalchemy.ext.hybrid` decorators
define the :meth:`.hybrid_property.expression` modifier for this purpose.   As an example we'll 
define the radius of the interval, which requires the usage of the absolute value function::

    from sqlalchemy import func
    
    class Interval(object):
        # ...
        
        @hybrid_property
        def radius(self):
            return abs(self.length) / 2
            
        @radius.expression
        def radius(cls):
            return func.abs(cls.length) / 2

Above the Python function ``abs()`` is used for instance-level operations, the SQL function
``ABS()`` is used via the :attr:`.func` object for class-level expressions::

    >>> i1.radius
    2
    
    >>> print Session().query(Interval).filter(Interval.radius > 5)
    SELECT interval.id AS interval_id, interval.start AS interval_start, 
        interval."end" AS interval_end 
    FROM interval 
    WHERE abs(interval."end" - interval.start) / :abs_1 > :param_1

Defining Setters
----------------

Hybrid properties can also define setter methods.  If we wanted ``length`` above, when 
set, to modify the endpoint value::

    class Interval(object):
        # ...
        
        @hybrid_property
        def length(self):
            return self.end - self.start

        @length.setter
        def length(self, value):
            self.end = self.start + value

The ``length(self, value)`` method is now called upon set::

    >>> i1 = Interval(5, 10)
    >>> i1.length
    5
    >>> i1.length = 12
    >>> i1.end
    17

Working with Relationships
--------------------------

There's no essential difference when creating hybrids that work with related objects as 
opposed to column-based data. The need for distinct expressions tends to be greater.
Consider the following declarative mapping which relates a ``User`` to a ``SavingsAccount``::

    from sqlalchemy import Column, Integer, ForeignKey, Numeric, String
    from sqlalchemy.orm import relationship
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.ext.hybrid import hybrid_property
    
    Base = declarative_base()
    
    class SavingsAccount(Base):
        __tablename__ = 'account'
        id = Column(Integer, primary_key=True)
        user_id = Column(Integer, ForeignKey('user.id'), nullable=False)
        balance = Column(Numeric(15, 5))

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String(100), nullable=False)
        
        accounts = relationship("SavingsAccount", backref="owner")
        
        @hybrid_property
        def balance(self):
            if self.accounts:
                return self.accounts[0].balance
            else:
                return None

        @balance.setter
        def balance(self, value):
            if not self.accounts:
                account = Account(owner=self)
            else:
                account = self.accounts[0]
            account.balance = balance

        @balance.expression
        def balance(cls):
            return SavingsAccount.balance

The above hybrid property ``balance`` works with the first ``SavingsAccount`` entry in the list of 
accounts for this user.   The in-Python getter/setter methods can treat ``accounts`` as a Python
list available on ``self``.  

However, at the expression level, we can't travel along relationships to column attributes 
directly since SQLAlchemy is explicit about joins.   So here, it's expected that the ``User`` class will be 
used in an appropriate context such that an appropriate join to ``SavingsAccount`` will be present::

    >>> print Session().query(User, User.balance).join(User.accounts).filter(User.balance > 5000)
    SELECT "user".id AS user_id, "user".name AS user_name, account.balance AS account_balance
    FROM "user" JOIN account ON "user".id = account.user_id 
    WHERE account.balance > :balance_1

Note however, that while the instance level accessors need to worry about whether ``self.accounts``
is even present, this issue expresses itself differently at the SQL expression level, where we basically
would use an outer join::

    >>> from sqlalchemy import or_
    >>> print Session().query(User, User.balance).outerjoin(User.accounts).\\
    ...         filter(or_(User.balance < 5000, User.balance == None))
    SELECT "user".id AS user_id, "user".name AS user_name, account.balance AS account_balance 
    FROM "user" LEFT OUTER JOIN account ON "user".id = account.user_id 
    WHERE account.balance <  :balance_1 OR account.balance IS NULL

.. _hybrid_custom_comparators:

Building Custom Comparators
---------------------------

The hybrid property also includes a helper that allows construction of custom comparators.
A comparator object allows one to customize the behavior of each SQLAlchemy expression
operator individually.  They are useful when creating custom types that have 
some highly idiosyncratic behavior on the SQL side.

The example class below allows case-insensitive comparisons on the attribute
named ``word_insensitive``::

    from sqlalchemy.ext.hybrid import Comparator
    
    class CaseInsensitiveComparator(Comparator):
        def __eq__(self, other):
            return func.lower(self.__clause_element__()) == func.lower(other)

    class SearchWord(Base):
        __tablename__ = 'searchword'
        id = Column(Integer, primary_key=True)
        word = Column(String(255), nullable=False)
        
        @hybrid_property
        def word_insensitive(self):
            return self.word.lower()
        
        @word_insensitive.comparator
        def word_insensitive(cls):
            return CaseInsensitiveComparator(cls.word)

Above, SQL expressions against ``word_insensitive`` will apply the ``LOWER()`` 
SQL function to both sides::

    >>> print Session().query(SearchWord).filter_by(word_insensitive="Trucks")
    SELECT searchword.id AS searchword_id, searchword.word AS searchword_word 
    FROM searchword 
    WHERE lower(searchword.word) = lower(:lower_1)

"""
from sqlalchemy import util
from sqlalchemy.orm import attributes, interfaces

class hybrid_method(object):
    """A decorator which allows definition of a Python object method with both
    instance-level and class-level behavior.
    
    """


    def __init__(self, func, expr=None):
        """Create a new :class:`.hybrid_method`.
        
        Usage is typically via decorator::
        
            from sqlalchemy.ext.hybrid import hybrid_method
        
            class SomeClass(object):
                @hybrid_method
                def value(self, x, y):
                    return self._value + x + y
            
                @value.expression
                def value(self, x, y):
                    return func.some_function(self._value, x, y)
            
        """
        self.func = func
        self.expr = expr or func

    def __get__(self, instance, owner):
        if instance is None:
            return self.expr.__get__(owner, owner.__class__)
        else:
            return self.func.__get__(instance, owner)

    def expression(self, expr):
        """Provide a modifying decorator that defines a SQL-expression producing method."""

        self.expr = expr
        return self

class hybrid_property(object):
    """A decorator which allows definition of a Python descriptor with both
    instance-level and class-level behavior.
    
    """

    def __init__(self, fget, fset=None, fdel=None, expr=None):
        """Create a new :class:`.hybrid_property`.
        
        Usage is typically via decorator::
        
            from sqlalchemy.ext.hybrid import hybrid_property
        
            class SomeClass(object):
                @hybrid_property
                def value(self):
                    return self._value
            
                @value.setter
                def value(self, value):
                    self._value = value
            
        """
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        self.expr = expr or fget
        util.update_wrapper(self, fget)

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
        """Provide a modifying decorator that defines a value-setter method."""

        self.fset = fset
        return self

    def deleter(self, fdel):
        """Provide a modifying decorator that defines a value-deletion method."""

        self.fdel = fdel
        return self

    def expression(self, expr):
        """Provide a modifying decorator that defines a SQL-expression producing method."""

        self.expr = expr
        return self

    def comparator(self, comparator):
        """Provide a modifying decorator that defines a custom comparator producing method.
        
        The return value of the decorated method should be an instance of
        :class:`~.hybrid.Comparator`.
        
        """

        proxy_attr = attributes.\
                        create_proxied_attribute(self)
        def expr(owner):
            return proxy_attr(owner, self.__name__, self, comparator(owner))
        self.expr = expr
        return self


class Comparator(interfaces.PropComparator):
    """A helper class that allows easy construction of custom :class:`~.orm.interfaces.PropComparator`
    classes for usage with hybrids."""


    def __init__(self, expression):
        self.expression = expression

    def __clause_element__(self):
        expr = self.expression
        while hasattr(expr, '__clause_element__'):
            expr = expr.__clause_element__()
        return expr

    def adapted(self, adapter):
        # interesting....
        return self


