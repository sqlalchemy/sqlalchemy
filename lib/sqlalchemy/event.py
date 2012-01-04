# sqlalchemy/event.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Base event API."""

from sqlalchemy import util, exc

CANCEL = util.symbol('CANCEL')
NO_RETVAL = util.symbol('NO_RETVAL')

def listen(target, identifier, fn, *args, **kw):
    """Register a listener function for the given target.
    
    e.g.::
    
        from sqlalchemy import event
        from sqlalchemy.schema import UniqueConstraint
        
        def unique_constraint_name(const, table):
            const.name = "uq_%s_%s" % (
                table.name,
                list(const.columns)[0].name
            )
        event.listen(
                UniqueConstraint, 
                "after_parent_attach", 
                unique_constraint_name)

    """

    for evt_cls in _registrars[identifier]:
        tgt = evt_cls._accept_with(target)
        if tgt is not None:
            tgt.dispatch._listen(tgt, identifier, fn, *args, **kw)
            return
    raise exc.InvalidRequestError("No such event '%s' for target '%s'" %
                                (identifier,target))

def listens_for(target, identifier, *args, **kw):
    """Decorate a function as a listener for the given target + identifier.
    
    e.g.::
    
        from sqlalchemy import event
        from sqlalchemy.schema import UniqueConstraint
        
        @event.listens_for(UniqueConstraint, "after_parent_attach")
        def unique_constraint_name(const, table):
            const.name = "uq_%s_%s" % (
                table.name,
                list(const.columns)[0].name
            )
    """
    def decorate(fn):
        listen(target, identifier, fn, *args, **kw)
        return fn
    return decorate

def remove(target, identifier, fn):
    """Remove an event listener.

    Note that some event removals, particularly for those event dispatchers
    which create wrapper functions and secondary even listeners, may not yet
    be supported.

    """
    for evt_cls in _registrars[identifier]:
        for tgt in evt_cls._accept_with(target):
            tgt.dispatch._remove(identifier, tgt, fn, *args, **kw)
            return

_registrars = util.defaultdict(list)

def _is_event_name(name):
    return not name.startswith('_') and name != 'dispatch'

class _UnpickleDispatch(object):
    """Serializable callable that re-generates an instance of :class:`_Dispatch`
    given a particular :class:`.Events` subclass.

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

    def __init__(self, _parent_cls):
        self._parent_cls = _parent_cls

    def __reduce__(self):
        return _UnpickleDispatch(), (self._parent_cls, )

    def _update(self, other, only_propagate=True):
        """Populate from the listeners in another :class:`_Dispatch`
            object."""

        for ls in _event_descriptors(other):
            getattr(self, ls.name)._update(ls, only_propagate=only_propagate)

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
    cls.dispatch = dispatch_cls = type("%sDispatch" % classname, 
                                        (dispatch_base, ), {})
    dispatch_cls._listen = cls._listen
    dispatch_cls._clear = cls._clear

    for k in dict_:
        if _is_event_name(k):
            setattr(dispatch_cls, k, _DispatchDescriptor(dict_[k]))
            _registrars[k].append(cls)

def _remove_dispatcher(cls):
    for k in dir(cls):
        if _is_event_name(k):
            _registrars[k].remove(cls)
            if not _registrars[k]:
                del _registrars[k]

class Events(object):
    """Define event listening functions for a particular target type."""


    __metaclass__ = _EventMeta

    @classmethod
    def _accept_with(cls, target):
        # Mapper, ClassManager, Session override this to
        # also accept classes, scoped_sessions, sessionmakers, etc.
        if hasattr(target, 'dispatch') and (
                    isinstance(target.dispatch, cls.dispatch) or \
                    isinstance(target.dispatch, type) and \
                    issubclass(target.dispatch, cls.dispatch)
                ):
            return target
        else:
            return None

    @classmethod
    def _listen(cls, target, identifier, fn, propagate=False, insert=False):
        if insert:
            getattr(target.dispatch, identifier).insert(fn, target, propagate)
        else:
            getattr(target.dispatch, identifier).append(fn, target, propagate)

    @classmethod
    def _remove(cls, target, identifier, fn):
        getattr(target.dispatch, identifier).remove(fn, target)

    @classmethod
    def _clear(cls):
        for attr in dir(cls.dispatch):
            if _is_event_name(attr):
                getattr(cls.dispatch, attr).clear()

class _DispatchDescriptor(object):
    """Class-level attributes on :class:`._Dispatch` classes."""

    def __init__(self, fn):
        self.__name__ = fn.__name__
        self.__doc__ = fn.__doc__
        self._clslevel = util.defaultdict(list)

    def insert(self, obj, target, propagate):
        assert isinstance(target, type), \
                "Class-level Event targets must be classes."

        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            self._clslevel[cls].insert(0, obj)

    def append(self, obj, target, propagate):
        assert isinstance(target, type), \
                "Class-level Event targets must be classes."

        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            self._clslevel[cls].append(obj)

    def remove(self, obj, target):
        stack = [target]
        while stack:
            cls = stack.pop(0)
            stack.extend(cls.__subclasses__())
            self._clslevel[cls].remove(obj)

    def clear(self):
        """Clear all class level listeners"""

        for dispatcher in self._clslevel.values():
            dispatcher[:] = []

    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__[self.__name__] = result = \
                            _ListenerCollection(self, obj._parent_cls)
        return result

class _ListenerCollection(object):
    """Instance-level attributes on instances of :class:`._Dispatch`.

    Represents a collection of listeners.

    """

    _exec_once = False

    def __init__(self, parent, target_cls):
        self.parent_listeners = parent._clslevel[target_cls]
        self.name = parent.__name__
        self.listeners = []
        self.propagate = set()

    def exec_once(self, *args, **kw):
        """Execute this event, but only if it has not been
        executed already for this collection."""

        if not self._exec_once:
            self(*args, **kw)
            self._exec_once = True

    def __call__(self, *args, **kw):
        """Execute this event."""

        for fn in self.parent_listeners:
            fn(*args, **kw)
        for fn in self.listeners:
            fn(*args, **kw)

    # I'm not entirely thrilled about the overhead here,
    # but this allows class-level listeners to be added
    # at any point.
    #
    # alternatively, _DispatchDescriptor could notify
    # all _ListenerCollection objects, but then we move
    # to a higher memory model, i.e.weakrefs to all _ListenerCollection
    # objects, the _DispatchDescriptor collection repeated
    # for all instances.

    def __len__(self):
        return len(self.parent_listeners + self.listeners)

    def __iter__(self):
        return iter(self.parent_listeners + self.listeners)

    def __getitem__(self, index):
        return (self.parent_listeners + self.listeners)[index]

    def __nonzero__(self):
        return bool(self.listeners or self.parent_listeners)

    def _update(self, other, only_propagate=True):
        """Populate from the listeners in another :class:`_Dispatch`
            object."""

        existing_listeners = self.listeners
        existing_listener_set = set(existing_listeners)
        self.propagate.update(other.propagate)
        existing_listeners.extend([l for l 
                                in other.listeners 
                                if l not in existing_listener_set
                                and not only_propagate or l in self.propagate
                                ])

    def insert(self, obj, target, propagate):
        if obj not in self.listeners:
            self.listeners.insert(0, obj)
            if propagate:
                self.propagate.add(obj)

    def append(self, obj, target, propagate):
        if obj not in self.listeners:
            self.listeners.append(obj)
            if propagate:
                self.propagate.add(obj)

    def remove(self, obj, target):
        if obj in self.listeners:
            self.listeners.remove(obj)
            self.propagate.discard(obj)

    def clear(self):
        self.listeners[:] = []
        self.propagate.clear()

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
