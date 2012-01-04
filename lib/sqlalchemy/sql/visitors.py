# sql/visitors.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Visitor/traversal interface and library functions.

SQLAlchemy schema and expression constructs rely on a Python-centric
version of the classic "visitor" pattern as the primary way in which
they apply functionality.  The most common use of this pattern 
is statement compilation, where individual expression classes match 
up to rendering methods that produce a string result.   Beyond this, 
the visitor system is also used to inspect expressions for various 
information and patterns, as well as for usage in 
some kinds of expression transformation.  Other kinds of transformation
use a non-visitor traversal system.

For many examples of how the visit system is used, see the 
sqlalchemy.sql.util and the sqlalchemy.sql.compiler modules.
For an introduction to clause adaption, see
http://techspot.zzzeek.org/2008/01/23/expression-transformations/

"""

from collections import deque
import re
from sqlalchemy import util
import operator

__all__ = ['VisitableType', 'Visitable', 'ClauseVisitor', 
    'CloningVisitor', 'ReplacingCloningVisitor', 'iterate', 
    'iterate_depthfirst', 'traverse_using', 'traverse',
    'cloned_traverse', 'replacement_traverse']

class VisitableType(type):
    """Metaclass which checks for a `__visit_name__` attribute and
    applies `_compiler_dispatch` method to classes.

    """

    def __init__(cls, clsname, bases, clsdict):
        if cls.__name__ == 'Visitable' or not hasattr(cls, '__visit_name__'):
            super(VisitableType, cls).__init__(clsname, bases, clsdict)
            return

        _generate_dispatch(cls)

        super(VisitableType, cls).__init__(clsname, bases, clsdict)

def _generate_dispatch(cls):
    # set up an optimized visit dispatch function
    # for use by the compiler
    if '__visit_name__' in cls.__dict__:
        visit_name = cls.__visit_name__
        if isinstance(visit_name, str):
            getter = operator.attrgetter("visit_%s" % visit_name)
            def _compiler_dispatch(self, visitor, **kw):
                return getter(visitor)(self, **kw)
        else:
            def _compiler_dispatch(self, visitor, **kw):
                return getattr(visitor, 'visit_%s' % self.__visit_name__)(self, **kw)

        cls._compiler_dispatch = _compiler_dispatch

class Visitable(object):
    """Base class for visitable objects, applies the
    ``VisitableType`` metaclass.

    """

    __metaclass__ = VisitableType

class ClauseVisitor(object):
    """Base class for visitor objects which can traverse using 
    the traverse() function.

    """

    __traverse_options__ = {}

    def traverse_single(self, obj, **kw):
        for v in self._visitor_iterator:
            meth = getattr(v, "visit_%s" % obj.__visit_name__, None)
            if meth:
                return meth(obj, **kw)

    def iterate(self, obj):
        """traverse the given expression structure, returning an iterator of all elements."""

        return iterate(obj, self.__traverse_options__)

    def traverse(self, obj):
        """traverse and visit the given expression structure."""

        return traverse(obj, self.__traverse_options__, self._visitor_dict)

    @util.memoized_property
    def _visitor_dict(self):
        visitors = {}

        for name in dir(self):
            if name.startswith('visit_'):
                visitors[name[6:]] = getattr(self, name)
        return visitors

    @property
    def _visitor_iterator(self):
        """iterate through this visitor and each 'chained' visitor."""

        v = self
        while v:
            yield v
            v = getattr(v, '_next', None)

    def chain(self, visitor):
        """'chain' an additional ClauseVisitor onto this ClauseVisitor.

        the chained visitor will receive all visit events after this one.

        """
        tail = list(self._visitor_iterator)[-1]
        tail._next = visitor
        return self

class CloningVisitor(ClauseVisitor):
    """Base class for visitor objects which can traverse using 
    the cloned_traverse() function.

    """

    def copy_and_process(self, list_):
        """Apply cloned traversal to the given list of elements, and return the new list."""

        return [self.traverse(x) for x in list_]

    def traverse(self, obj):
        """traverse and visit the given expression structure."""

        return cloned_traverse(obj, self.__traverse_options__, self._visitor_dict)

class ReplacingCloningVisitor(CloningVisitor):
    """Base class for visitor objects which can traverse using 
    the replacement_traverse() function.

    """

    def replace(self, elem):
        """receive pre-copied elements during a cloning traversal.

        If the method returns a new element, the element is used 
        instead of creating a simple copy of the element.  Traversal 
        will halt on the newly returned element if it is re-encountered.
        """
        return None

    def traverse(self, obj):
        """traverse and visit the given expression structure."""

        def replace(elem):
            for v in self._visitor_iterator:
                e = v.replace(elem)
                if e is not None:
                    return e
        return replacement_traverse(obj, self.__traverse_options__, replace)

def iterate(obj, opts):
    """traverse the given expression structure, returning an iterator.

    traversal is configured to be breadth-first.

    """
    stack = deque([obj])
    while stack:
        t = stack.popleft()
        yield t
        for c in t.get_children(**opts):
            stack.append(c)

def iterate_depthfirst(obj, opts):
    """traverse the given expression structure, returning an iterator.

    traversal is configured to be depth-first.

    """
    stack = deque([obj])
    traversal = deque()
    while stack:
        t = stack.pop()
        traversal.appendleft(t)
        for c in t.get_children(**opts):
            stack.append(c)
    return iter(traversal)

def traverse_using(iterator, obj, visitors):
    """visit the given expression structure using the given iterator of objects."""

    for target in iterator:
        meth = visitors.get(target.__visit_name__, None)
        if meth:
            meth(target)
    return obj

def traverse(obj, opts, visitors):
    """traverse and visit the given expression structure using the default iterator."""

    return traverse_using(iterate(obj, opts), obj, visitors)

def traverse_depthfirst(obj, opts, visitors):
    """traverse and visit the given expression structure using the depth-first iterator."""

    return traverse_using(iterate_depthfirst(obj, opts), obj, visitors)

def cloned_traverse(obj, opts, visitors):
    """clone the given expression structure, allowing 
    modifications by visitors."""

    cloned = util.column_dict()
    stop_on = util.column_set(opts.get('stop_on', []))

    def clone(elem):
        if elem in stop_on:
            return elem
        else:
            if elem not in cloned:
                cloned[elem] = newelem = elem._clone()
                newelem._copy_internals(clone=clone)
                meth = visitors.get(newelem.__visit_name__, None)
                if meth:
                    meth(newelem)
            return cloned[elem]

    if obj is not None:
        obj = clone(obj)
    return obj


def replacement_traverse(obj, opts, replace):
    """clone the given expression structure, allowing element 
    replacement by a given replacement function."""

    cloned = util.column_dict()
    stop_on = util.column_set(opts.get('stop_on', []))

    def clone(elem, **kw):
        if elem in stop_on or \
            'no_replacement_traverse' in elem._annotations:
            return elem
        else:
            newelem = replace(elem)
            if newelem is not None:
                stop_on.add(newelem)
                return newelem
            else:
                if elem not in cloned:
                    cloned[elem] = newelem = elem._clone()
                    newelem._copy_internals(clone=clone, **kw)
                return cloned[elem]

    if obj is not None:
        obj = clone(obj, **opts)
    return obj
