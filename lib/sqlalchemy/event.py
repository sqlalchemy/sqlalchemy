"""
The event system handles all events throughout the sqlalchemy
and sqlalchemy.orm packages.   

Event specifications:

:attr:`sqlalchemy.pool.Pool.events`

"""

from sqlalchemy import util

def listen(fn, identifier, target, *args):
    """Listen for events, passing to fn."""
    
    getattr(target.events, identifier).append(fn, target)

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
    

class _ExecEvent(object):
    def exec_and_clear(self, *args, **kw):
        """Execute the given event once, then clear all listeners."""
        
        self(*args, **kw)
        self[:] = []
        
    def __call__(self, *args, **kw):
        """Execute the given event."""
        
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
        self._clslevel.append((obj, target))
    
    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__[self.__name__] = result = Listeners()
        result.extend([
            fn for fn, target in 
            self._clslevel
            if issubclass(obj.parent_cls, target)
        ])
        return result

class Listeners(_ExecEvent, list):
    """Represent a collection of listeners linked
    to an instance of :class:`Events`."""
    
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
