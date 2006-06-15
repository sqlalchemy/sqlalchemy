# util.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import thread, threading, weakref, UserList, time, string, inspect, sys, sets
from exceptions import *
import __builtin__

try:
    Set = set
except:
    Set = sets.Set
    
def to_list(x):
    if x is None:
        return None
    if not isinstance(x, list) and not isinstance(x, tuple):
        return [x]
    else:
        return x

def to_set(x):
    if x is None:
        return Set()
    if not isinstance(x, Set):
        return Set(to_list(x))
    else:
        return x

def reversed(seq):
    try:
        return __builtin__.reversed(seq)
    except:
        def rev():
            i = len(seq) -1
            while  i >= 0:
                yield seq[i]
                i -= 1
            raise StopIteration()
        return rev()

class ArgSingleton(type):
    instances = {}
    def __call__(self, *args):
        hashkey = (self, args)
        try:
            return ArgSingleton.instances[hashkey]
        except KeyError:
            instance = type.__call__(self, *args)
            ArgSingleton.instances[hashkey] = instance
            return instance
        
class SimpleProperty(object):
    """a "default" property accessor."""
    def __init__(self, key):
        self.key = key
    def __set__(self, obj, value):
        setattr(obj, self.key, value)
    def __delete__(self, obj):
        delattr(obj, self.key)
    def __get__(self, obj, owner):
        if obj is None:
            return self
        else:
            return getattr(obj, self.key)

class Logger(object):
    """defines various forms of logging"""
    def __init__(self, logger=None, usethreads=False, usetimestamp=True, origin=None):
        self.logger = logger or sys.stdout
        self.usethreads = usethreads
        self.usetimestamp = usetimestamp
        self.origin = origin
    def write(self, msg):
        if self.usetimestamp:
            t = time.time()
            ms = (t - long(t)) * 1000
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
            timestamp = "[%s,%03d]" % (timestamp, ms)
        else:
            timestamp = None
        if self.origin:
            origin = "[%s]" % self.origin
            origin = "%-8s" % origin
        else:
            origin = None
        if self.usethreads:
            threadname = threading.currentThread().getName()
            threadname = "[" + threadname + ' '*(8-len(threadname)) + "]"
        else:
            threadname = None
        self.logger.write(string.join([s for s in (timestamp, threadname, origin) if s is not None]) + ": " + msg + "\n")
    
class OrderedProperties(object):
    """
    An object that maintains the order in which attributes are set upon it.
    also provides an iterator and a very basic getitem/setitem interface to those attributes.
    
    (Not really a dict, since it iterates over values, not keys.  Not really
    a list, either, since each value must have a key associated; hence there is
    no append or extend.)
    """
    def __init__(self):
        self.__dict__['_OrderedProperties__data'] = OrderedDict()
    def __len__(self):
        return len(self.__data)
    def __iter__(self):
        return self.__data.itervalues()
    def __setitem__(self, key, object):
        self.__data[key] = object
    def __getitem__(self, key):
        return self.__data[key]
    def __delitem__(self, key):
        del self.__data[key]
    def __setattr__(self, key, object):
        self.__data[key] = object
    def __getattr__(self, key):
        try:
            return self.__data[key]
        except KeyError:
            raise AttributeError(key)
    def keys(self):
        return self.__data.keys()
    def has_key(self, key):
        return self.__data.has_key(key)
    def clear(self):
        self.__data.clear()
        
class OrderedDict(dict):
    """A Dictionary that keeps its own internal ordering"""
    
    def __init__(self, values = None):
        self._list = []
        if values is not None:
            for val in values:
                self.update(val)
    def keys(self):
        return list(self._list)
    def clear(self):
        self._list = []
        dict.clear(self)
    def update(self, dict):
        for key in dict.keys():
            self.__setitem__(key, dict[key])
    def setdefault(self, key, value):
        if not self.has_key(key):
            self.__setitem__(key, value)
            return value
        else:
            return self.__getitem__(key)
    def values(self):
        return map(lambda key: self[key], self._list)
    def __iter__(self):
        return iter(self._list)
    def itervalues(self):
        return iter([self[key] for key in self._list])
    def iterkeys(self): 
        return self.__iter__()
    def iteritems(self):
        return iter([(key, self[key]) for key in self.keys()])
    def __delitem__(self, key):
        try:
            del self._list[self._list.index(key)]
        except ValueError:
            raise KeyError(key)
        dict.__delitem__(self, key)
    def __setitem__(self, key, object):
        if not self.has_key(key):
            self._list.append(key)
        dict.__setitem__(self, key, object)
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

class ThreadLocal(object):
    """an object in which attribute access occurs only within the context of the current thread"""
    def __init__(self):
        self.__dict__['_tdict'] = {}
    def __delattr__(self, key):
        try:
            del self._tdict["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            raise AttributeError(key)
    def __getattr__(self, key):
        try:
            return self._tdict["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            raise AttributeError(key)
    def __setattr__(self, key, value):
        self._tdict["%d_%s" % (thread.get_ident(), key)] = value

class DictDecorator(dict):
    """a Dictionary that delegates items not found to a second wrapped dictionary."""
    def __init__(self, decorate):
        self.decorate = decorate
    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.decorate[key]
    def __repr__(self):
        return dict.__repr__(self) + repr(self.decorate)

class OrderedSet(sets.Set):
    def __init__(self, iterable=None):
        """Construct a set from an optional iterable."""
        self._data = OrderedDict()
        if iterable is not None: 
          self._update(iterable)

class UniqueAppender(object):
    def __init__(self, data):
        self.data = data
        if hasattr(data, 'append'):
            self._data_appender = data.append
        elif hasattr(data, 'add'):
            self._data_appender = data.add
        self.set = Set()
    def append(self, item):
        if item not in self.set:
            self.set.add(item)
            self._data_appender(item)
        
class ScopedRegistry(object):
    """a Registry that can store one or multiple instances of a single class 
    on a per-thread scoped basis, or on a customized scope
    
    createfunc - a callable that returns a new object to be placed in the registry
    scopefunc - a callable that will return a key to store/retrieve an object,
    defaults to thread.get_ident for thread-local objects.  use a value like
    lambda: True for application scope.
    """
    def __init__(self, createfunc, scopefunc=None):
        self.createfunc = createfunc
        if scopefunc is None:
            self.scopefunc = thread.get_ident
        else:
            self.scopefunc = scopefunc
        self.registry = {}
    def __call__(self):
        key = self._get_key()
        try:
            return self.registry[key]
        except KeyError:
            return self.registry.setdefault(key, self.createfunc())
    def set(self, obj):
        self.registry[self._get_key()] = obj
    def clear(self):
        try:
            del self.registry[self._get_key()]
        except KeyError:
            pass
    def _get_key(self):
        return self.scopefunc()


def constructor_args(instance, **kwargs):
    """given an object instance and keyword arguments, inspects the 
    argument signature of the instance's __init__ method and returns 
    a tuple of list and keyword arguments, suitable for creating a new
    instance of the class.  The returned arguments are drawn from the
    given keyword dictionary, or if not found are drawn from the 
    corresponding attributes of the original instance."""
    classobj = instance.__class__
        
    argspec = inspect.getargspec(classobj.__init__.im_func)

    argnames = argspec[0] or []
    defaultvalues = argspec[3] or []

    (requiredargs, namedargs) = (
            argnames[0:len(argnames) - len(defaultvalues)], 
            argnames[len(argnames) - len(defaultvalues):]
            )

    newparams = {}

    for arg in requiredargs:
        if arg == 'self': 
            continue
        elif kwargs.has_key(arg):
            newparams[arg] = kwargs[arg]
        else:
            newparams[arg] = getattr(instance, arg)

    for arg in namedargs:
        if kwargs.has_key(arg):
            newparams[arg] = kwargs[arg]
        else:
            if hasattr(instance, arg):
                newparams[arg] = getattr(instance, arg)
            else:
                raise AssertionError("instance has no attribute '%s'" % arg)

    return newparams
    