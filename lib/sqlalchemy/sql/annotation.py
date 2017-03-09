# sql/annotation.py
# Copyright (C) 2005-2017 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The :class:`.Annotated` class and related routines; creates hash-equivalent
copies of SQL constructs which contain context-specific markers and
associations.

"""

from .. import util
from . import operators


class Annotated(object):
    """clones a ClauseElement and applies an 'annotations' dictionary.

    Unlike regular clones, this clone also mimics __hash__() and
    __cmp__() of the original element so that it takes its place
    in hashed collections.

    A reference to the original element is maintained, for the important
    reason of keeping its hash value current.  When GC'ed, the
    hash value may be reused, causing conflicts.

    .. note::  The rationale for Annotated producing a brand new class,
       rather than placing the functionality directly within ClauseElement,
       is **performance**.  The __hash__() method is absent on plain
       ClauseElement which leads to significantly reduced function call
       overhead, as the use of sets and dictionaries against ClauseElement
       objects is prevalent, but most are not "annotated".

    """

    def __new__(cls, *args):
        if not args:
            # clone constructor
            return object.__new__(cls)
        else:
            element, values = args
            # pull appropriate subclass from registry of annotated
            # classes
            try:
                cls = annotated_classes[element.__class__]
            except KeyError:
                cls = _new_annotation_type(element.__class__, cls)
            return object.__new__(cls)

    def __init__(self, element, values):
        self.__dict__ = element.__dict__.copy()
        self.__element = element
        self._annotations = values
        self._hash = hash(element)

    def _annotate(self, values):
        _values = self._annotations.copy()
        _values.update(values)
        return self._with_annotations(_values)

    def _with_annotations(self, values):
        clone = self.__class__.__new__(self.__class__)
        clone.__dict__ = self.__dict__.copy()
        clone._annotations = values
        return clone

    def _deannotate(self, values=None, clone=True):
        if values is None:
            return self.__element
        else:
            _values = self._annotations.copy()
            for v in values:
                _values.pop(v, None)
            return self._with_annotations(_values)

    def _compiler_dispatch(self, visitor, **kw):
        return self.__element.__class__._compiler_dispatch(
            self, visitor, **kw)

    @property
    def _constructor(self):
        return self.__element._constructor

    def _clone(self):
        clone = self.__element._clone()
        if clone is self.__element:
            # detect immutable, don't change anything
            return self
        else:
            # update the clone with any changes that have occurred
            # to this object's __dict__.
            clone.__dict__.update(self.__dict__)
            return self.__class__(clone, self._annotations)

    def __hash__(self):
        return self._hash

    def __eq__(self, other):
        if isinstance(self.__element, operators.ColumnOperators):
            return self.__element.__class__.__eq__(self, other)
        else:
            return hash(other) == hash(self)


# hard-generate Annotated subclasses.  this technique
# is used instead of on-the-fly types (i.e. type.__new__())
# so that the resulting objects are pickleable.
annotated_classes = {}


def _deep_annotate(element, annotations, exclude=None):
    """Deep copy the given ClauseElement, annotating each element
    with the given annotations dictionary.

    Elements within the exclude collection will be cloned but not annotated.

    """
    def clone(elem):
        if exclude and \
                hasattr(elem, 'proxy_set') and \
                elem.proxy_set.intersection(exclude):
            newelem = elem._clone()
        elif annotations != elem._annotations:
            newelem = elem._annotate(annotations)
        else:
            newelem = elem
        newelem._copy_internals(clone=clone)
        return newelem

    if element is not None:
        element = clone(element)
    return element


def _deep_deannotate(element, values=None):
    """Deep copy the given element, removing annotations."""

    cloned = util.column_dict()

    def clone(elem):
        # if a values dict is given,
        # the elem must be cloned each time it appears,
        # as there may be different annotations in source
        # elements that are remaining.  if totally
        # removing all annotations, can assume the same
        # slate...
        if values or elem not in cloned:
            newelem = elem._deannotate(values=values, clone=True)
            newelem._copy_internals(clone=clone)
            if not values:
                cloned[elem] = newelem
            return newelem
        else:
            return cloned[elem]

    if element is not None:
        element = clone(element)
    return element


def _shallow_annotate(element, annotations):
    """Annotate the given ClauseElement and copy its internals so that
    internal objects refer to the new annotated object.

    Basically used to apply a "dont traverse" annotation to a
    selectable, without digging throughout the whole
    structure wasting time.
    """
    element = element._annotate(annotations)
    element._copy_internals()
    return element


def _new_annotation_type(cls, base_cls):
    if issubclass(cls, Annotated):
        return cls
    elif cls in annotated_classes:
        return annotated_classes[cls]

    for super_ in cls.__mro__:
        # check if an Annotated subclass more specific than
        # the given base_cls is already registered, such
        # as AnnotatedColumnElement.
        if super_ in annotated_classes:
            base_cls = annotated_classes[super_]
            break

    annotated_classes[cls] = anno_cls = type(
        "Annotated%s" % cls.__name__,
        (base_cls, cls), {})
    globals()["Annotated%s" % cls.__name__] = anno_cls
    return anno_cls


def _prepare_annotations(target_hierarchy, base_cls):
    stack = [target_hierarchy]
    while stack:
        cls = stack.pop()
        stack.extend(cls.__subclasses__())

        _new_annotation_type(cls, base_cls)
