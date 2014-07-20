# event/base.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Base implementation classes.

The public-facing ``Events`` serves as the base class for an event interface;
its public attributes represent different kinds of events.   These attributes
are mirrored onto a ``_Dispatch`` class, which serves as a container for
collections of listener functions.   These collections are represented both
at the class level of a particular ``_Dispatch`` class as well as within
instances of ``_Dispatch``.

"""
from __future__ import absolute_import

from .. import util
from .attr import _JoinedDispatchDescriptor, \
    _EmptyListener, _DispatchDescriptor

_registrars = util.defaultdict(list)


def _is_event_name(name):
    return not name.startswith('_') and name != 'dispatch'


class _UnpickleDispatch(object):
    """Serializable callable that re-generates an instance of
    :class:`_Dispatch` given a particular :class:`.Events` subclass.

    """

    def __call__(self, _parent_cls):
        for cls in _parent_cls.__mro__:
            if 'dispatch' in cls.__dict__:
                return cls.__dict__['dispatch'].dispatch_cls(_parent_cls)
        else:
            raise AttributeError("No class with a 'dispatch' member present.")


class _Dispatch(object):
    """Mirror the event listening definitions of an Events class with
    listener collections.

    Classes which define a "dispatch" member will return a
    non-instantiated :class:`._Dispatch` subclass when the member
    is accessed at the class level.  When the "dispatch" member is
    accessed at the instance level of its owner, an instance
    of the :class:`._Dispatch` class is returned.

    A :class:`._Dispatch` class is generated for each :class:`.Events`
    class defined, by the :func:`._create_dispatcher_class` function.
    The original :class:`.Events` classes remain untouched.
    This decouples the construction of :class:`.Events` subclasses from
    the implementation used by the event internals, and allows
    inspecting tools like Sphinx to work in an unsurprising
    way against the public API.

    """

    _events = None
    """reference the :class:`.Events` class which this
        :class:`._Dispatch` is created for."""

    def __init__(self, _parent_cls):
        self._parent_cls = _parent_cls

    @util.classproperty
    def _listen(cls):
        return cls._events._listen

    def _join(self, other):
        """Create a 'join' of this :class:`._Dispatch` and another.

        This new dispatcher will dispatch events to both
        :class:`._Dispatch` objects.

        """
        if '_joined_dispatch_cls' not in self.__class__.__dict__:
            cls = type(
                "Joined%s" % self.__class__.__name__,
                (_JoinedDispatcher, self.__class__), {}
            )
            for ls in _event_descriptors(self):
                setattr(cls, ls.name, _JoinedDispatchDescriptor(ls.name))

            self.__class__._joined_dispatch_cls = cls
        return self._joined_dispatch_cls(self, other)

    def __reduce__(self):
        return _UnpickleDispatch(), (self._parent_cls, )

    def _update(self, other, only_propagate=True):
        """Populate from the listeners in another :class:`_Dispatch`
            object."""

        for ls in _event_descriptors(other):
            if isinstance(ls, _EmptyListener):
                continue
            getattr(self, ls.name).\
                for_modify(self)._update(ls, only_propagate=only_propagate)

    @util.hybridmethod
    def _clear(self):
        for attr in dir(self):
            if _is_event_name(attr):
                getattr(self, attr).for_modify(self).clear()


def _event_descriptors(target):
    return [getattr(target, k) for k in dir(target) if _is_event_name(k)]


class _EventMeta(type):
    """Intercept new Event subclasses and create
    associated _Dispatch classes."""

    def __init__(cls, classname, bases, dict_):
        _create_dispatcher_class(cls, classname, bases, dict_)
        return type.__init__(cls, classname, bases, dict_)


def _create_dispatcher_class(cls, classname, bases, dict_):
    """Create a :class:`._Dispatch` class corresponding to an
    :class:`.Events` class."""

    # there's all kinds of ways to do this,
    # i.e. make a Dispatch class that shares the '_listen' method
    # of the Event class, this is the straight monkeypatch.
    dispatch_base = getattr(cls, 'dispatch', _Dispatch)
    dispatch_cls = type("%sDispatch" % classname,
                        (dispatch_base, ), {})
    cls._set_dispatch(cls, dispatch_cls)

    for k in dict_:
        if _is_event_name(k):
            setattr(dispatch_cls, k, _DispatchDescriptor(cls, dict_[k]))
            _registrars[k].append(cls)

    if getattr(cls, '_dispatch_target', None):
        cls._dispatch_target.dispatch = dispatcher(cls)


def _remove_dispatcher(cls):
    for k in dir(cls):
        if _is_event_name(k):
            _registrars[k].remove(cls)
            if not _registrars[k]:
                del _registrars[k]


class Events(util.with_metaclass(_EventMeta, object)):
    """Define event listening functions for a particular target type."""

    @staticmethod
    def _set_dispatch(cls, dispatch_cls):
        # this allows an Events subclass to define additional utility
        # methods made available to the target via
        # "self.dispatch._events.<utilitymethod>"
        # @staticemethod to allow easy "super" calls while in a metaclass
        # constructor.
        cls.dispatch = dispatch_cls
        dispatch_cls._events = cls

    @classmethod
    def _accept_with(cls, target):
        # Mapper, ClassManager, Session override this to
        # also accept classes, scoped_sessions, sessionmakers, etc.
        if hasattr(target, 'dispatch') and (
            isinstance(target.dispatch, cls.dispatch) or
            isinstance(target.dispatch, type) and
            issubclass(target.dispatch, cls.dispatch)
        ):
            return target
        else:
            return None

    @classmethod
    def _listen(cls, event_key, propagate=False, insert=False, named=False):
        event_key.base_listen(propagate=propagate, insert=insert, named=named)

    @classmethod
    def _remove(cls, event_key):
        event_key.remove()

    @classmethod
    def _clear(cls):
        cls.dispatch._clear()


class _JoinedDispatcher(object):
    """Represent a connection between two _Dispatch objects."""

    def __init__(self, local, parent):
        self.local = local
        self.parent = parent
        self._parent_cls = local._parent_cls


class dispatcher(object):
    """Descriptor used by target classes to
    deliver the _Dispatch class at the class level
    and produce new _Dispatch instances for target
    instances.

    """

    def __init__(self, events):
        self.dispatch_cls = events.dispatch
        self.events = events

    def __get__(self, obj, cls):
        if obj is None:
            return self.dispatch_cls
        obj.__dict__['dispatch'] = disp = self.dispatch_cls(cls)
        return disp
