# exc.py - ORM exceptions
# Copyright (C) the SQLAlchemy authors and contributors
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""SQLAlchemy ORM exceptions."""

import sqlalchemy as sa


NO_STATE = (AttributeError, KeyError)
"""Exception types that may be raised by instrumentation implementations."""

class ConcurrentModificationError(sa.exc.SQLAlchemyError):
    """Rows have been modified outside of the unit of work."""


class FlushError(sa.exc.SQLAlchemyError):
    """A invalid condition was detected during flush()."""


class UnmappedError(sa.exc.InvalidRequestError):
    """TODO"""

class DetachedInstanceError(sa.exc.SQLAlchemyError):
    """An attempt to access unloaded attributes on a mapped instance that is detached."""
    
class UnmappedInstanceError(UnmappedError):
    """An mapping operation was requested for an unknown instance."""

    def __init__(self, obj, msg=None):
        if not msg:
            try:
                mapper = sa.orm.class_mapper(type(obj))
                name = _safe_cls_name(type(obj))
                msg = ("Class %r is mapped, but this instance lacks "
                       "instrumentation.  This occurs when the instance is created "
                       "before sqlalchemy.orm.mapper(%s) was called." % (name, name))
            except UnmappedClassError:
                msg = _default_unmapped(type(obj))
                if isinstance(obj, type):
                    msg += (
                        '; was a class (%s) supplied where an instance was '
                        'required?' % _safe_cls_name(obj))
        UnmappedError.__init__(self, msg)


class UnmappedClassError(UnmappedError):
    """An mapping operation was requested for an unknown class."""

    def __init__(self, cls, msg=None):
        if not msg:
            msg = _default_unmapped(cls)
        UnmappedError.__init__(self, msg)


class ObjectDeletedError(sa.exc.InvalidRequestError):
    """An refresh() operation failed to re-retrieve an object's row."""


class UnmappedColumnError(sa.exc.InvalidRequestError):
    """Mapping operation was requested on an unknown column."""


class NoResultFound(sa.exc.InvalidRequestError):
    """A database result was required but none was found."""


class MultipleResultsFound(sa.exc.InvalidRequestError):
    """A single database result was required but more than one were found."""


# Legacy compat until 0.6.
sa.exc.ConcurrentModificationError = ConcurrentModificationError
sa.exc.FlushError = FlushError
sa.exc.UnmappedColumnError

def _safe_cls_name(cls):
    try:
        cls_name = '.'.join((cls.__module__, cls.__name__))
    except AttributeError:
        cls_name = getattr(cls, '__name__', None)
        if cls_name is None:
            cls_name = repr(cls)
    return cls_name

def _default_unmapped(cls):
    try:
        mappers = sa.orm.attributes.manager_of_class(cls).mappers
    except NO_STATE:
        mappers = {}
    except TypeError:
        mappers = {}
    name = _safe_cls_name(cls)

    if not mappers:
        return "Class '%s' is not mapped" % name
