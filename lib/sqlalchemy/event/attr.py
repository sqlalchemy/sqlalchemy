# event/attr.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Attribute implementation for _Dispatch classes.

The various listener targets for a particular event class are represented
as attributes, which refer to collections of listeners to be fired off.
These collections can exist at the class level as well as at the instance
level.  An event is fired off using code like this::

    some_object.dispatch.first_connect(arg1, arg2)

Above, ``some_object.dispatch`` would be an instance of ``_Dispatch`` and
``first_connect`` is typically an instance of ``_ListenerCollection``
if event listeners are present, or ``_EmptyListener`` if none are present.

The attribute mechanics here spend effort trying to ensure listener functions
are available with a minimum of function call overhead, that unnecessary
objects aren't created (i.e. many empty per-instance listener collections),
as well as that everything is garbage collectable when owning references are
lost.  Other features such as "propagation" of listener functions across
many ``_Dispatch`` instances, "joining" of multiple ``_Dispatch`` instances,
as well as support for subclass propagation (e.g. events assigned to
``Pool`` vs. ``QueuePool``) are all implemented here.

"""

from __future__ import absolute_import, with_statement

from .. import util
from ..util import threading
from . import registry
from . import legacy
from itertools import chain
import weakref


class RefCollection(object):
    @util.memoized_property
    def ref(self):
        return weakref.ref(self, registry._collection_gced)


class _DispatchDescriptor(RefCollection):
    """Class-level attributes on :class:`._Dispatch` classes."""

    def __init__(self, parent_dispatch_cls, fn):
        self.__name__ = fn.__name__
        argspec = util.inspect_getargspec(fn)
        self.arg_names = argspec.args[1:]
        self.has_kw = bool(argspec.keywords)
        self.legacy_signatures = list(reversed(
            sorted(
                getattr(fn, '_legacy_signatures', []),
                key=lambda s: s[0]
            )
        ))
        self.__doc__ = fn.__doc__ = legacy._augment_fn_docs(
            self, parent_dispatch_cls, fn)

        self._clslevel = weakref.WeakKeyDictionary()
        self._empty_listeners = weakref.WeakKeyDictionary()

    def _adjust_fn_spec(self, fn, named):
        if named:
            fn = self._wrap_fn_for_kw(fn)
        if self.legacy_signatures:
            try:
                argspec = util.get_callable_argspec(fn, no_self=True)
            except TypeError:
                pass
            else:
                fn = legacy._wrap_fn_for_legacy(self, fn, argspec)
        return fn

    def _wrap_fn_for_kw(self, fn):
        def wrap_kw(*args, **kw):
            argdict = dict(zip(self.arg_names, args))
            argdict.update(kw)
            return fn(**argdict)
        return wrap_kw

    def insert(self, event_key, propagate):
        target = event_key.dispatch_target
        assert isinstance(target, type), \
            "Class-level Event targets must be classes."
        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            if cls is not target and cls not in self._clslevel:
                self.update_subclass(cls)
            else:
                if cls not in self._clslevel:
                    self._clslevel[cls] = []
                self._clslevel[cls].insert(0, event_key._listen_fn)
        registry._stored_in_collection(event_key, self)

    def append(self, event_key, propagate):
        target = event_key.dispatch_target
        assert isinstance(target, type), \
            "Class-level Event targets must be classes."

        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            if cls is not target and cls not in self._clslevel:
                self.update_subclass(cls)
            else:
                if cls not in self._clslevel:
                    self._clslevel[cls] = []
                self._clslevel[cls].append(event_key._listen_fn)
        registry._stored_in_collection(event_key, self)

    def update_subclass(self, target):
        if target not in self._clslevel:
            self._clslevel[target] = []
        clslevel = self._clslevel[target]
        for cls in target.__mro__[1:]:
            if cls in self._clslevel:
                clslevel.extend([
                    fn for fn
                    in self._clslevel[cls]
                    if fn not in clslevel
                ])

    def remove(self, event_key):
        target = event_key.dispatch_target
        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            if cls in self._clslevel:
                self._clslevel[cls].remove(event_key._listen_fn)
        registry._removed_from_collection(event_key, self)

    def clear(self):
        """Clear all class level listeners"""

        to_clear = set()
        for dispatcher in self._clslevel.values():
            to_clear.update(dispatcher)
            dispatcher[:] = []
        registry._clear(self, to_clear)

    def for_modify(self, obj):
        """Return an event collection which can be modified.

        For _DispatchDescriptor at the class level of
        a dispatcher, this returns self.

        """
        return self

    def __get__(self, obj, cls):
        if obj is None:
            return self
        elif obj._parent_cls in self._empty_listeners:
            ret = self._empty_listeners[obj._parent_cls]
        else:
            self._empty_listeners[obj._parent_cls] = ret = \
                _EmptyListener(self, obj._parent_cls)
        # assigning it to __dict__ means
        # memoized for fast re-access.  but more memory.
        obj.__dict__[self.__name__] = ret
        return ret


class _HasParentDispatchDescriptor(object):
    def _adjust_fn_spec(self, fn, named):
        return self.parent._adjust_fn_spec(fn, named)


class _EmptyListener(_HasParentDispatchDescriptor):
    """Serves as a class-level interface to the events
    served by a _DispatchDescriptor, when there are no
    instance-level events present.

    Is replaced by _ListenerCollection when instance-level
    events are added.

    """

    def __init__(self, parent, target_cls):
        if target_cls not in parent._clslevel:
            parent.update_subclass(target_cls)
        self.parent = parent  # _DispatchDescriptor
        self.parent_listeners = parent._clslevel[target_cls]
        self.name = parent.__name__
        self.propagate = frozenset()
        self.listeners = ()

    def for_modify(self, obj):
        """Return an event collection which can be modified.

        For _EmptyListener at the instance level of
        a dispatcher, this generates a new
        _ListenerCollection, applies it to the instance,
        and returns it.

        """
        result = _ListenerCollection(self.parent, obj._parent_cls)
        if obj.__dict__[self.name] is self:
            obj.__dict__[self.name] = result
        return result

    def _needs_modify(self, *args, **kw):
        raise NotImplementedError("need to call for_modify()")

    exec_once = insert = append = remove = clear = _needs_modify

    def __call__(self, *args, **kw):
        """Execute this event."""

        for fn in self.parent_listeners:
            fn(*args, **kw)

    def __len__(self):
        return len(self.parent_listeners)

    def __iter__(self):
        return iter(self.parent_listeners)

    def __bool__(self):
        return bool(self.parent_listeners)

    __nonzero__ = __bool__


class _CompoundListener(_HasParentDispatchDescriptor):
    _exec_once = False

    @util.memoized_property
    def _exec_once_mutex(self):
        return threading.Lock()

    def exec_once(self, *args, **kw):
        """Execute this event, but only if it has not been
        executed already for this collection."""

        if not self._exec_once:
            with self._exec_once_mutex:
                if not self._exec_once:
                    try:
                        self(*args, **kw)
                    finally:
                        self._exec_once = True

    def __call__(self, *args, **kw):
        """Execute this event."""

        for fn in self.parent_listeners:
            fn(*args, **kw)
        for fn in self.listeners:
            fn(*args, **kw)

    def __len__(self):
        return len(self.parent_listeners) + len(self.listeners)

    def __iter__(self):
        return chain(self.parent_listeners, self.listeners)

    def __bool__(self):
        return bool(self.listeners or self.parent_listeners)

    __nonzero__ = __bool__


class _ListenerCollection(RefCollection, _CompoundListener):
    """Instance-level attributes on instances of :class:`._Dispatch`.

    Represents a collection of listeners.

    As of 0.7.9, _ListenerCollection is only first
    created via the _EmptyListener.for_modify() method.

    """

    def __init__(self, parent, target_cls):
        if target_cls not in parent._clslevel:
            parent.update_subclass(target_cls)
        self.parent_listeners = parent._clslevel[target_cls]
        self.parent = parent
        self.name = parent.__name__
        self.listeners = []
        self.propagate = set()

    def for_modify(self, obj):
        """Return an event collection which can be modified.

        For _ListenerCollection at the instance level of
        a dispatcher, this returns self.

        """
        return self

    def _update(self, other, only_propagate=True):
        """Populate from the listeners in another :class:`_Dispatch`
            object."""

        existing_listeners = self.listeners
        existing_listener_set = set(existing_listeners)
        self.propagate.update(other.propagate)
        other_listeners = [l for l
                           in other.listeners
                           if l not in existing_listener_set
                           and not only_propagate or l in self.propagate
                           ]

        existing_listeners.extend(other_listeners)

        to_associate = other.propagate.union(other_listeners)
        registry._stored_in_collection_multi(self, other, to_associate)

    def insert(self, event_key, propagate):
        if event_key.prepend_to_list(self, self.listeners):
            if propagate:
                self.propagate.add(event_key._listen_fn)

    def append(self, event_key, propagate):
        if event_key.append_to_list(self, self.listeners):
            if propagate:
                self.propagate.add(event_key._listen_fn)

    def remove(self, event_key):
        self.listeners.remove(event_key._listen_fn)
        self.propagate.discard(event_key._listen_fn)
        registry._removed_from_collection(event_key, self)

    def clear(self):
        registry._clear(self, self.listeners)
        self.propagate.clear()
        self.listeners[:] = []


class _JoinedDispatchDescriptor(object):
    def __init__(self, name):
        self.name = name

    def __get__(self, obj, cls):
        if obj is None:
            return self
        else:
            obj.__dict__[self.name] = ret = _JoinedListener(
                obj.parent, self.name,
                getattr(obj.local, self.name)
            )
            return ret


class _JoinedListener(_CompoundListener):
    _exec_once = False

    def __init__(self, parent, name, local):
        self.parent = parent
        self.name = name
        self.local = local
        self.parent_listeners = self.local

    @property
    def listeners(self):
        return getattr(self.parent, self.name)

    def _adjust_fn_spec(self, fn, named):
        return self.local._adjust_fn_spec(fn, named)

    def for_modify(self, obj):
        self.local = self.parent_listeners = self.local.for_modify(obj)
        return self

    def insert(self, event_key, propagate):
        self.local.insert(event_key, propagate)

    def append(self, event_key, propagate):
        self.local.append(event_key, propagate)

    def remove(self, event_key):
        self.local.remove(event_key)

    def clear(self):
        raise NotImplementedError()
