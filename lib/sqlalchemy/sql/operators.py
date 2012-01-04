# sql/operators.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines operators used in SQL expressions."""

from operator import (
    and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg
    )

# Py2K
from operator import (div,)
# end Py2K

from sqlalchemy.util import symbol

class Operators(object):
    """Base of comparison and logical operators.
    
    Implements base methods :meth:`operate` and :meth:`reverse_operate`,
    as well as :meth:`__and__`, :meth:`__or__`, :meth:`__invert__`.
    
    Usually is used via its most common subclass
    :class:`.ColumnOperators`.
    
    """
    def __and__(self, other):
        """Implement the ``&`` operator.
        
        When used with SQL expressions, results in an
        AND operation, equivalent to
        :func:`~.expression.and_`, that is::
        
            a & b
        
        is equivalent to::
        
            from sqlalchemy import and_
            and_(a, b)

        Care should be taken when using ``&`` regarding
        operator precedence; the ``&`` operator has the highest precedence.
        The operands should be enclosed in parenthesis if they contain
        further sub expressions::
        
            (a == 2) & (b == 4)

        """
        return self.operate(and_, other)

    def __or__(self, other):
        """Implement the ``|`` operator.
        
        When used with SQL expressions, results in an
        OR operation, equivalent to
        :func:`~.expression.or_`, that is::
        
            a | b
        
        is equivalent to::
        
            from sqlalchemy import or_
            or_(a, b)

        Care should be taken when using ``|`` regarding
        operator precedence; the ``|`` operator has the highest precedence.
        The operands should be enclosed in parenthesis if they contain
        further sub expressions::
        
            (a == 2) | (b == 4)

        """
        return self.operate(or_, other)

    def __invert__(self):
        """Implement the ``~`` operator.
        
        When used with SQL expressions, results in a 
        NOT operation, equivalent to 
        :func:`~.expression.not_`, that is::
        
            ~a
            
        is equivalent to::
        
            from sqlalchemy import not_
            not_(a)

        """
        return self.operate(inv)

    def op(self, opstring):
        """produce a generic operator function.

        e.g.::

          somecolumn.op("*")(5)

        produces::

          somecolumn * 5

        :param operator: a string which will be output as the infix operator
          between this :class:`.ClauseElement` and the expression passed to the
          generated function.

        This function can also be used to make bitwise operators explicit. For
        example::

          somecolumn.op('&')(0xff)

        is a bitwise AND of the value in somecolumn.

        """
        def _op(b):
            return self.operate(op, opstring, b)
        return _op

    def operate(self, op, *other, **kwargs):
        """Operate on an argument.
        
        This is the lowest level of operation, raises
        :class:`NotImplementedError` by default.
        
        Overriding this on a subclass can allow common 
        behavior to be applied to all operations.  
        For example, overriding :class:`.ColumnOperators`
        to apply ``func.lower()`` to the left and right 
        side::
        
            class MyComparator(ColumnOperators):
                def operate(self, op, other):
                    return op(func.lower(self), func.lower(other))

        :param op:  Operator callable.
        :param \*other: the 'other' side of the operation. Will
         be a single scalar for most operations.
        :param \**kwargs: modifiers.  These may be passed by special
         operators such as :meth:`ColumnOperators.contains`.
        
        
        """
        raise NotImplementedError(str(op))

    def reverse_operate(self, op, other, **kwargs):
        """Reverse operate on an argument.
        
        Usage is the same as :meth:`operate`.
        
        """
        raise NotImplementedError(str(op))

class ColumnOperators(Operators):
    """Defines comparison and math operations.
    
    By default all methods call down to
    :meth:`Operators.operate` or :meth:`Operators.reverse_operate`
    passing in the appropriate operator function from the 
    Python builtin ``operator`` module or
    a SQLAlchemy-specific operator function from 
    :mod:`sqlalchemy.expression.operators`.   For example
    the ``__eq__`` function::
    
        def __eq__(self, other):
            return self.operate(operators.eq, other)

    Where ``operators.eq`` is essentially::
    
        def eq(a, b):
            return a == b
    
    A SQLAlchemy construct like :class:`.ColumnElement` ultimately
    overrides :meth:`.Operators.operate` and others
    to return further :class:`.ClauseElement` constructs, 
    so that the ``==`` operation above is replaced by a clause
    construct.
    
    The docstrings here will describe column-oriented
    behavior of each operator.  For ORM-based operators
    on related objects and collections, see :class:`.RelationshipProperty.Comparator`.
    
    """

    timetuple = None
    """Hack, allows datetime objects to be compared on the LHS."""

    def __lt__(self, other):
        """Implement the ``<`` operator.
        
        In a column context, produces the clause ``a < b``.
        
        """
        return self.operate(lt, other)

    def __le__(self, other):
        """Implement the ``<=`` operator.
        
        In a column context, produces the clause ``a <= b``.
        
        """
        return self.operate(le, other)

    __hash__ = Operators.__hash__

    def __eq__(self, other):
        """Implement the ``==`` operator.
        
        In a column context, produces the clause ``a = b``.
        If the target is ``None``, produces ``a IS NULL``.

        """
        return self.operate(eq, other)

    def __ne__(self, other):
        """Implement the ``!=`` operator.

        In a column context, produces the clause ``a != b``.
        If the target is ``None``, produces ``a IS NOT NULL``.
        
        """
        return self.operate(ne, other)

    def __gt__(self, other):
        """Implement the ``>`` operator.
        
        In a column context, produces the clause ``a > b``.
        
        """
        return self.operate(gt, other)

    def __ge__(self, other):
        """Implement the ``>=`` operator.
        
        In a column context, produces the clause ``a >= b``.
        
        """
        return self.operate(ge, other)

    def __neg__(self):
        """Implement the ``-`` operator.
        
        In a column context, produces the clause ``-a``.
        
        """
        return self.operate(neg)

    def concat(self, other):
        """Implement the 'concat' operator.
        
        In a column context, produces the clause ``a || b``,
        or uses the ``concat()`` operator on MySQL.
        
        """
        return self.operate(concat_op, other)

    def like(self, other, escape=None):
        """Implement the ``like`` operator.
        
        In a column context, produces the clause ``a LIKE other``.
        
        """
        return self.operate(like_op, other, escape=escape)

    def ilike(self, other, escape=None):
        """Implement the ``ilike`` operator.
        
        In a column context, produces the clause ``a ILIKE other``.
        
        """
        return self.operate(ilike_op, other, escape=escape)

    def in_(self, other):
        """Implement the ``in`` operator.
        
        In a column context, produces the clause ``a IN other``.
        "other" may be a tuple/list of column expressions,
        or a :func:`~.expression.select` construct.
        
        """
        return self.operate(in_op, other)

    def startswith(self, other, **kwargs):
        """Implement the ``startwith`` operator.

        In a column context, produces the clause ``LIKE '<other>%'``
        
        """
        return self.operate(startswith_op, other, **kwargs)

    def endswith(self, other, **kwargs):
        """Implement the 'endswith' operator.
        
        In a column context, produces the clause ``LIKE '%<other>'``
        
        """
        return self.operate(endswith_op, other, **kwargs)

    def contains(self, other, **kwargs):
        """Implement the 'contains' operator.
        
        In a column context, produces the clause ``LIKE '%<other>%'``
        
        """
        return self.operate(contains_op, other, **kwargs)

    def match(self, other, **kwargs):
        """Implements the 'match' operator.
        
        In a column context, this produces a MATCH clause, i.e. 
        ``MATCH '<other>'``.  The allowed contents of ``other`` 
        are database backend specific.

        """
        return self.operate(match_op, other, **kwargs)

    def desc(self):
        """Produce a :func:`~.expression.desc` clause against the
        parent object."""
        return self.operate(desc_op)

    def asc(self):
        """Produce a :func:`~.expression.asc` clause against the
        parent object."""
        return self.operate(asc_op)

    def nullsfirst(self):
        """Produce a :func:`~.expression.nullsfirst` clause against the
        parent object."""
        return self.operate(nullsfirst_op)

    def nullslast(self):
        """Produce a :func:`~.expression.nullslast` clause against the
        parent object."""
        return self.operate(nullslast_op)

    def collate(self, collation):
        """Produce a :func:`~.expression.collate` clause against
        the parent object, given the collation string."""
        return self.operate(collate, collation)

    def __radd__(self, other):
        """Implement the ``+`` operator in reverse.

        See :meth:`__add__`.
        
        """
        return self.reverse_operate(add, other)

    def __rsub__(self, other):
        """Implement the ``-`` operator in reverse.

        See :meth:`__sub__`.
        
        """
        return self.reverse_operate(sub, other)

    def __rmul__(self, other):
        """Implement the ``*`` operator in reverse.

        See :meth:`__mul__`.
        
        """
        return self.reverse_operate(mul, other)

    def __rdiv__(self, other):
        """Implement the ``/`` operator in reverse.

        See :meth:`__div__`.
        
        """
        return self.reverse_operate(div, other)

    def between(self, cleft, cright):
        """Produce a :func:`~.expression.between` clause against
        the parent object, given the lower and upper range."""
        return self.operate(between_op, cleft, cright)

    def distinct(self):
        """Produce a :func:`~.expression.distinct` clause against the parent object."""
        return self.operate(distinct_op)

    def __add__(self, other):
        """Implement the ``+`` operator.
        
        In a column context, produces the clause ``a + b``
        if the parent object has non-string affinity.
        If the parent object has a string affinity, 
        produces the concatenation operator, ``a || b`` -
        see :meth:`concat`.
        
        """
        return self.operate(add, other)

    def __sub__(self, other):
        """Implement the ``-`` operator.
        
        In a column context, produces the clause ``a - b``.
        
        """
        return self.operate(sub, other)

    def __mul__(self, other):
        """Implement the ``*`` operator.
        
        In a column context, produces the clause ``a * b``.
        
        """
        return self.operate(mul, other)

    def __div__(self, other):
        """Implement the ``/`` operator.
        
        In a column context, produces the clause ``a / b``.
        
        """
        return self.operate(div, other)

    def __mod__(self, other):
        """Implement the ``%`` operator.
        
        In a column context, produces the clause ``a % b``.
        
        """
        return self.operate(mod, other)

    def __truediv__(self, other):
        """Implement the ``//`` operator.
        
        In a column context, produces the clause ``a / b``.
        
        """
        return self.operate(truediv, other)

    def __rtruediv__(self, other):
        """Implement the ``//`` operator in reverse.
        
        See :meth:`__truediv__`.
        
        """
        return self.reverse_operate(truediv, other)

def from_():
    raise NotImplementedError()

def as_():
    raise NotImplementedError()

def exists():
    raise NotImplementedError()

def is_(a, b):
    return a.is_(b)

def isnot(a, b):
    return a.isnot(b)

def collate(a, b):
    return a.collate(b)

def op(a, opstring, b):
    return a.op(opstring)(b)

def like_op(a, b, escape=None):
    return a.like(b, escape=escape)

def notlike_op(a, b, escape=None):
    raise NotImplementedError()

def ilike_op(a, b, escape=None):
    return a.ilike(b, escape=escape)

def notilike_op(a, b, escape=None):
    raise NotImplementedError()

def between_op(a, b, c):
    return a.between(b, c)

def in_op(a, b):
    return a.in_(b)

def notin_op(a, b):
    raise NotImplementedError()

def distinct_op(a):
    return a.distinct()

def startswith_op(a, b, escape=None):
    return a.startswith(b, escape=escape)

def endswith_op(a, b, escape=None):
    return a.endswith(b, escape=escape)

def contains_op(a, b, escape=None):
    return a.contains(b, escape=escape)

def match_op(a, b):
    return a.match(b)

def comma_op(a, b):
    raise NotImplementedError()

def concat_op(a, b):
    return a.concat(b)

def desc_op(a):
    return a.desc()

def asc_op(a):
    return a.asc()

def nullsfirst_op(a):
    return a.nullsfirst()

def nullslast_op(a):
    return a.nullslast()

_commutative = set([eq, ne, add, mul])

def is_commutative(op):
    return op in _commutative

def is_ordering_modifier(op):
    return op in (asc_op, desc_op, 
                    nullsfirst_op, nullslast_op)

_associative = _commutative.union([concat_op, and_, or_])


_smallest = symbol('_smallest')
_largest = symbol('_largest')

_PRECEDENCE = {
    from_: 15,
    mul: 7,
    truediv: 7,
    # Py2K
    div: 7,
    # end Py2K
    mod: 7,
    neg: 7,
    add: 6,
    sub: 6,
    concat_op: 6,
    match_op: 6,
    ilike_op: 5,
    notilike_op: 5,
    like_op: 5,
    notlike_op: 5,
    in_op: 5,
    notin_op: 5,
    is_: 5,
    isnot: 5,
    eq: 5,
    ne: 5,
    gt: 5,
    lt: 5,
    ge: 5,
    le: 5,
    between_op: 5,
    distinct_op: 5,
    inv: 5,
    and_: 3,
    or_: 2,
    comma_op: -1,
    collate: 7,
    as_: -1,
    exists: 0,
    _smallest: -1000,
    _largest: 1000
}

def is_precedent(operator, against):
    if operator is against and operator in _associative:
        return False
    else:
        return (_PRECEDENCE.get(operator, _PRECEDENCE[_smallest]) <=
            _PRECEDENCE.get(against, _PRECEDENCE[_largest]))
