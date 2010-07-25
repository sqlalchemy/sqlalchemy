"""
The event system handles all events throughout the sqlalchemy
and sqlalchemy.orm packages.   

Event specifications:

:attr:`sqlalchemy.pool.Pool.events`

"""

from sqlalchemy import util

def listen(fn, identifier, target, *args, **kw):
    """Listen for events, passing to fn."""
    
    target.events.listen(fn, identifier, target, *args, **kw)

NO_RESULT = util.symbol('no_result')

class _DispatchMeta(type):
    def __init__(cls, classname, bases, dict_):
        for k in dict_:
            if k.startswith('on_'):
                setattr(cls, k, EventDescriptor(dict_[k]))
        return type.__init__(cls, classname, bases, dict_)

class Events(object):
    __metaclass__ = _DispatchMeta
    
    def __init__(self, parent_cls):
        self.parent_cls = parent_cls
    
    @classmethod
    def listen(cls, fn, identifier, target):
        getattr(target.events, identifier).append(fn, target)
    
    @property
    def events(self):
        """Iterate the Listeners objects."""
        
        return (getattr(self, k) for k in dir(self) if k.startswith("on_"))
        
    def update(self, other):
        """Populate from the listeners in another :class:`Events` object."""

        for ls in other.events:
            getattr(self, ls.name).extend(ls)

class _ExecEvent(object):
    _exec_once = False
    
    def exec_once(self, *args, **kw):
        """Execute this event, but only if it has not been
        executed already for this collection."""
        
        if not self._exec_once:
            self(*args, **kw)
            self._exec_once = True
    
    def exec_until_return(self, *args, **kw):
        """Execute listeners for this event until
        one returns a non-None value.
        
        Returns the value, or None.
        """
        
        if self:
            for fn in self:
                r = fn(*args, **kw)
                if r is not None:
                    return r
        return None
        
    def __call__(self, *args, **kw):
        """Execute this event."""
        
        if self:
            for fn in self:
                fn(*args, **kw)
    
class EventDescriptor(object):
    """Represent an event type associated with a :class:`Events` class
    as well as class-level listeners.
    
    """
    def __init__(self, fn):
        self.__name__ = fn.__name__
        self.__doc__ = fn.__doc__
        self._clslevel = []
    
    def append(self, obj, target):
        assert isinstance(target, type), "Class-level Event targets must be classes."
        self._clslevel.append((obj, target))
    
    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__[self.__name__] = result = Listeners(self.__name__)
        result.extend([
            fn for fn, target in 
            self._clslevel
            if issubclass(obj.parent_cls, target)
        ])
        return result

class Listeners(_ExecEvent, list):
    """Represent a collection of listeners linked
    to an instance of :class:`Events`."""
    
    def __init__(self, name):
        self.name = name
        
    def append(self, obj, target):
        list.append(self, obj)

class dispatcher(object):
    def __init__(self, events):
        self.dispatch_cls = events
        
    def __get__(self, obj, cls):
        if obj is None:
            return self.dispatch_cls
        obj.__dict__['events'] = disp = self.dispatch_cls(cls)
        return disp
