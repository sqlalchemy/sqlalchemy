# sql/visitors.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
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
information and patterns, as well as for the purposes of applying
transformations to expressions.

Examples of how the visit system is used can be seen in the source code
of for example the ``sqlalchemy.sql.util`` and the ``sqlalchemy.sql.compiler``
modules.  Some background on clause adaption is also at
http://techspot.zzzeek.org/2008/01/23/expression-transformations/ .

"""

from collections import deque
import operator

from .. import exc
from .. import util


__all__ = [
    "VisitableType",
    "Visitable",
    "ClauseVisitor",
    "CloningVisitor",
    "ReplacingCloningVisitor",
    "iterate",
    "iterate_depthfirst",
    "traverse_using",
    "traverse",
    "traverse_depthfirst",
    "cloned_traverse",
    "replacement_traverse",
]


class VisitableType(type):
    """Metaclass which assigns a ``_compiler_dispatch`` method to classes
    having a ``__visit_name__`` attribute.

    The ``_compiler_dispatch`` attribute becomes an instance method which
    looks approximately like the following::

        def _compiler_dispatch (self, visitor, **kw):
            '''Look for an attribute named "visit_" + self.__visit_name__
            on the visitor, and call it with the same kw params.'''
            visit_attr = 'visit_%s' % self.__visit_name__
            return getattr(visitor, visit_attr)(self, **kw)

    Classes having no ``__visit_name__`` attribute will remain unaffected.

    """

    def __init__(cls, clsname, bases, clsdict):
        if clsname != "Visitable" and hasattr(cls, "__visit_name__"):
            _generate_dispatch(cls)

        super(VisitableType, cls).__init__(clsname, bases, clsdict)


def _generate_dispatch(cls):
    """Return an optimized visit dispatch function for the cls
    for use by the compiler.

    """
    if "__visit_name__" in cls.__dict__:
        visit_name = cls.__visit_name__

        if isinstance(visit_name, util.compat.string_types):
            # There is an optimization opportunity here because the
            # the string name of the class's __visit_name__ is known at
            # this early stage (import time) so it can be pre-constructed.
            getter = operator.attrgetter("visit_%s" % visit_name)

            def _compiler_dispatch(self, visitor, **kw):
                try:
                    meth = getter(visitor)
                except AttributeError as err:
                    util.raise_(
                        exc.UnsupportedCompilationError(visitor, cls),
                        replace_context=err,
                    )
                else:
                    return meth(self, **kw)

        else:
            # The optimization opportunity is lost for this case because the
            # __visit_name__ is not yet a string. As a result, the visit
            # string has to be recalculated with each compilation.
            def _compiler_dispatch(self, visitor, **kw):
                visit_attr = "visit_%s" % self.__visit_name__
                try:
                    meth = getattr(visitor, visit_attr)
                except AttributeError as err:
                    util.raise_(
                        exc.UnsupportedCompilationError(visitor, cls),
                        replace_context=err,
                    )
                else:
                    return meth(self, **kw)

        _compiler_dispatch.__doc__ = """Look for an attribute named "visit_" + self.__visit_name__
            on the visitor, and call it with the same kw params.
            """
        cls._compiler_dispatch = _compiler_dispatch


class Visitable(util.with_metaclass(VisitableType, object)):
    """Base class for visitable objects, applies the
    :class:`.visitors.VisitableType` metaclass.

    The :class:`.Visitable` class is essentially at the base of the
    :class:`_expression.ClauseElement` hierarchy.

    """


class ClauseVisitor(object):
    """Base class for visitor objects which can traverse using
    the :func:`.visitors.traverse` function.

    Direct usage of the :func:`.visitors.traverse` function is usually
    preferred.

    """

    __traverse_options__ = {}

    def traverse_single(self, obj, **kw):
        for v in self.visitor_iterator:
            meth = getattr(v, "visit_%s" % obj.__visit_name__, None)
            if meth:
                return meth(obj, **kw)

    def iterate(self, obj):
        """Traverse the given expression structure, returning an iterator
        of all elements.

        """
        return iterate(obj, self.__traverse_options__)

    def traverse(self, obj):
        """Traverse and visit the given expression structure."""

        return traverse(obj, self.__traverse_options__, self._visitor_dict)

    @util.memoized_property
    def _visitor_dict(self):
        visitors = {}

        for name in dir(self):
            if name.startswith("visit_"):
                visitors[name[6:]] = getattr(self, name)
        return visitors

    @property
    def visitor_iterator(self):
        """Iterate through this visitor and each 'chained' visitor."""

        v = self
        while v:
            yield v
            v = getattr(v, "_next", None)

    def chain(self, visitor):
        """'Chain' an additional ClauseVisitor onto this ClauseVisitor.

        The chained visitor will receive all visit events after this one.

        """
        tail = list(self.visitor_iterator)[-1]
        tail._next = visitor
        return self


class CloningVisitor(ClauseVisitor):
    """Base class for visitor objects which can traverse using
    the :func:`.visitors.cloned_traverse` function.

    Direct usage of the :func:`.visitors.cloned_traverse` function is usually
    preferred.


    """

    def copy_and_process(self, list_):
        """Apply cloned traversal to the given list of elements, and return
        the new list.

        """
        return [self.traverse(x) for x in list_]

    def traverse(self, obj):
        """Traverse and visit the given expression structure."""

        return cloned_traverse(
            obj, self.__traverse_options__, self._visitor_dict
        )


class ReplacingCloningVisitor(CloningVisitor):
    """Base class for visitor objects which can traverse using
    the :func:`.visitors.replacement_traverse` function.

    Direct usage of the :func:`.visitors.replacement_traverse` function is
    usually preferred.

    """

    def replace(self, elem):
        """Receive pre-copied elements during a cloning traversal.

        If the method returns a new element, the element is used
        instead of creating a simple copy of the element.  Traversal
        will halt on the newly returned element if it is re-encountered.
        """
        return None

    def traverse(self, obj):
        """Traverse and visit the given expression structure."""

        def replace(elem):
            for v in self.visitor_iterator:
                e = v.replace(elem)
                if e is not None:
                    return e

        return replacement_traverse(obj, self.__traverse_options__, replace)


def iterate(obj, opts):
    r"""Traverse the given expression structure, returning an iterator.

    Traversal is configured to be breadth-first.

    The central API feature used by the :func:`.visitors.iterate` and
    :func:`.visitors.iterate_depthfirst` functions is the
    :meth:`_expression.ClauseElement.get_children` method of
    :class:`_expression.ClauseElement` objects.  This method should return all
    the :class:`_expression.ClauseElement` objects which are associated with a
    particular :class:`_expression.ClauseElement` object. For example, a
    :class:`.Case` structure will refer to a series of
    :class:`_expression.ColumnElement` objects within its "whens" and "else\_"
    member variables.

    :param obj: :class:`_expression.ClauseElement` structure to be traversed

    :param opts: dictionary of iteration options.   This dictionary is usually
     empty in modern usage.

    """
    # fasttrack for atomic elements like columns
    children = obj.get_children(**opts)
    if not children:
        return [obj]

    traversal = deque()
    stack = deque([obj])
    while stack:
        t = stack.popleft()
        traversal.append(t)
        for c in t.get_children(**opts):
            stack.append(c)
    return iter(traversal)


def iterate_depthfirst(obj, opts):
    """Traverse the given expression structure, returning an iterator.

    Traversal is configured to be depth-first.

    :param obj: :class:`_expression.ClauseElement` structure to be traversed

    :param opts: dictionary of iteration options.   This dictionary is usually
     empty in modern usage.

    .. seealso::

        :func:`.visitors.iterate` - includes a general overview of iteration.

    """
    # fasttrack for atomic elements like columns
    children = obj.get_children(**opts)
    if not children:
        return [obj]

    stack = deque([obj])
    traversal = deque()
    while stack:
        t = stack.pop()
        traversal.appendleft(t)
        for c in t.get_children(**opts):
            stack.append(c)
    return iter(traversal)


def traverse_using(iterator, obj, visitors):
    """Visit the given expression structure using the given iterator of
    objects.

    :func:`.visitors.traverse_using` is usually called internally as the result
    of the :func:`.visitors.traverse` or :func:`.visitors.traverse_depthfirst`
    functions.

    :param iterator: an iterable or sequence which will yield
     :class:`_expression.ClauseElement`
     structures; the iterator is assumed to be the
     product of the :func:`.visitors.iterate` or
     :func:`.visitors.iterate_depthfirst` functions.

    :param obj: the :class:`_expression.ClauseElement`
     that was used as the target of the
     :func:`.iterate` or :func:`.iterate_depthfirst` function.

    :param visitors: dictionary of visit functions.  See :func:`.traverse`
     for details on this dictionary.

    .. seealso::

        :func:`.traverse`

        :func:`.traverse_depthfirst`

    """
    for target in iterator:
        meth = visitors.get(target.__visit_name__, None)
        if meth:
            meth(target)
    return obj


def traverse(obj, opts, visitors):
    """Traverse and visit the given expression structure using the default
    iterator.

     e.g.::

        from sqlalchemy.sql import visitors

        stmt = select([some_table]).where(some_table.c.foo == 'bar')

        def visit_bindparam(bind_param):
            print("found bound value: %s" % bind_param.value)

        visitors.traverse(stmt, {}, {"bindparam": visit_bindparam})

    The iteration of objects uses the :func:`.visitors.iterate` function,
    which does a breadth-first traversal using a stack.

    :param obj: :class:`_expression.ClauseElement` structure to be traversed

    :param opts: dictionary of iteration options.   This dictionary is usually
     empty in modern usage.

    :param visitors: dictionary of visit functions.   The dictionary should
     have strings as keys, each of which would correspond to the
     ``__visit_name__`` of a particular kind of SQL expression object, and
     callable functions  as values, each of which represents a visitor function
     for that kind of object.

    """
    return traverse_using(iterate(obj, opts), obj, visitors)


def traverse_depthfirst(obj, opts, visitors):
    """traverse and visit the given expression structure using the
    depth-first iterator.

    The iteration of objects uses the :func:`.visitors.iterate_depthfirst`
    function, which does a depth-first traversal using a stack.

    Usage is the same as that of :func:`.visitors.traverse` function.


    """
    return traverse_using(iterate_depthfirst(obj, opts), obj, visitors)


def cloned_traverse(obj, opts, visitors):
    """Clone the given expression structure, allowing modifications by
    visitors.

    Traversal usage is the same as that of :func:`.visitors.traverse`.
    The visitor functions present in the ``visitors`` dictionary may also
    modify the internals of the given structure as the traversal proceeds.

    The central API feature used by the :func:`.visitors.cloned_traverse`
    and :func:`.visitors.replacement_traverse` functions, in addition to the
    :meth:`_expression.ClauseElement.get_children`
    function that is used to achieve
    the iteration, is the :meth:`_expression.ClauseElement._copy_internals`
    method.
    For a :class:`_expression.ClauseElement`
    structure to support cloning and replacement
    traversals correctly, it needs to be able to pass a cloning function into
    its internal members in order to make copies of them.

    .. seealso::

        :func:`.visitors.traverse`

        :func:`.visitors.replacement_traverse`

    """

    cloned = {}
    stop_on = set(opts.get("stop_on", []))

    def clone(elem, **kw):
        if elem in stop_on:
            return elem
        else:
            if id(elem) not in cloned:
                cloned[id(elem)] = newelem = elem._clone()
                newelem._copy_internals(clone=clone, **kw)
                meth = visitors.get(newelem.__visit_name__, None)
                if meth:
                    meth(newelem)
            return cloned[id(elem)]

    if obj is not None:
        obj = clone(obj)
    clone = None  # remove gc cycles
    return obj


def replacement_traverse(obj, opts, replace):
    """Clone the given expression structure, allowing element
    replacement by a given replacement function.

    This function is very similar to the :func:`.visitors.cloned_traverse`
    function, except instead of being passed a dictionary of visitors, all
    elements are unconditionally passed into the given replace function.
    The replace function then has the option to return an entirely new object
    which will replace the one given.  If it returns ``None``, then the object
    is kept in place.

    The difference in usage between :func:`.visitors.cloned_traverse` and
    :func:`.visitors.replacement_traverse` is that in the former case, an
    already-cloned object is passed to the visitor function, and the visitor
    function can then manipulate the internal state of the object.
    In the case of the latter, the visitor function should only return an
    entirely different object, or do nothing.

    The use case for :func:`.visitors.replacement_traverse` is that of
    replacing a FROM clause inside of a SQL structure with a different one,
    as is a common use case within the ORM.

    """

    cloned = {}
    stop_on = {id(x) for x in opts.get("stop_on", [])}

    def clone(elem, **kw):
        if (
            id(elem) in stop_on
            or "no_replacement_traverse" in elem._annotations
        ):
            return elem
        else:
            newelem = replace(elem)
            if newelem is not None:
                stop_on.add(id(newelem))
                return newelem
            else:
                if elem not in cloned:
                    cloned[elem] = newelem = elem._clone()
                    newelem._copy_internals(clone=clone, **kw)
                return cloned[elem]

    if obj is not None:
        obj = clone(obj, **opts)
    clone = None  # remove gc cycles
    return obj
