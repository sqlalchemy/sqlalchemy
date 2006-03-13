# util.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

__all__ = ['OrderedProperties', 'OrderedDict', 'generic_repr', 'HashSet', 'AttrProp']
import thread, threading, weakref, UserList, time, string, inspect, sys
from exceptions import *
import __builtin__

def to_list(x):
    if x is None:
        return None
    if not isinstance(x, list) and not isinstance(x, tuple):
        return [x]
    else:
        return x

def to_set(x):
    if x is None:
        return HashSet()
    if not isinstance(x, HashSet):
        return HashSet(to_list(x))
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
        
class AttrProp(object):
    """a quick way to stick a property accessor on an object"""
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
    
def generic_repr(obj, exclude=None):
    L = ['%s=%s' % (a, repr(getattr(obj, a))) for a in dir(obj) if not callable(getattr(obj, a)) and not a.startswith('_') and (exclude is None or not exclude.has_key(a))]
    return '%s(%s)' % (obj.__class__.__name__, ','.join(L))

def hash_key(obj):
    if obj is None:
        return 'None'
    elif isinstance(obj, list):
        return repr([hash_key(o) for o in obj])
    elif hasattr(obj, 'hash_key'):
        return obj.hash_key()
    else:
        return repr(obj)

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
        self.__dict__['_list'] = []
    def __len__(self):
        return len(self._list)
    def keys(self):
        return list(self._list)
    def get(self, key, default):
        return getattr(self, key, default)
    def has_key(self, key):
        return hasattr(self, key)
    def __iter__(self):
        return iter([self[x] for x in self._list])
    def __setitem__(self, key, object):
        setattr(self, key, object)
    def __getitem__(self, key):
        try:
          return getattr(self, key)
        except AttributeError:
          raise KeyError(key)
    def __delitem__(self, key):
        delattr(self, key)
        del self._list[self._list.index(key)]
    def __setattr__(self, key, object):
        if not hasattr(self, key):
            self._list.append(key)
        self.__dict__[key] = object
    def clear(self):
        self.__dict__.clear()
        self.__dict__['_list'] = []

class RecursionStack(object):
    """a thread-local stack used to detect recursive object traversals."""
    def __init__(self):
        self.stacks = {}
    def _get_stack(self):
        try:
            stack = self.stacks[thread.get_ident()]
        except KeyError:
            stack = {}
            self.stacks[thread.get_ident()] = stack
        return stack
    def push(self, obj):
        s = self._get_stack()
        if s.has_key(obj):
            return True
        else:
            s[obj] = True
            return False
    def pop(self, obj):
        stack = self._get_stack()
        del stack[obj]
        if len(stack) == 0:
            del self.stacks[thread.get_ident()]
        
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
    def __init__(self, raiseerror = True):
        self.__dict__['_tdict'] = {}
        self.__dict__['_raiseerror'] = raiseerror
    def __hasattr__(self, key):
        return self._tdict.has_key("%d_%s" % (thread.get_ident(), key))
    def __delattr__(self, key):
        try:
            del self._tdict["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            raise AttributeError(key)
    def __getattr__(self, key):
        try:
            return self._tdict["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            if self._raiseerror:
                raise AttributeError(key)
            else:
                return None
    def __setattr__(self, key, value):
        self._tdict["%d_%s" % (thread.get_ident(), key)] = value

class DictDecorator(dict):
    def __init__(self, decorate):
        self.decorate = decorate
    def __getitem__(self, key):
        try:
            return dict.__getitem__(self, key)
        except KeyError:
            return self.decorate[key]
    def __repr__(self):
        return dict.__repr__(self) + repr(self.decorate)
class HashSet(object):
    """implements a Set."""
    def __init__(self, iter=None, ordered=False):
        if ordered:
            self.map = OrderedDict()
        else:
            self.map  = {}
        if iter is not None:
            for i in iter:
                self.append(i)
    def __iter__(self):
        return iter(self.map.values())
    def contains(self, item):
        return self.map.has_key(item)
    def clear(self):
        self.map.clear()
    def empty(self):
        return len(self.map) == 0
    def append(self, item):
        self.map[item] = item
    def remove(self, item):
        del self.map[item]
    def __add__(self, other):
        return HashSet(self.map.values() + [i for i in other])
    def __len__(self):
        return len(self.map)
    def __delitem__(self, key):
        del self.map[key]
    def __getitem__(self, key):
        return self.map[key]
    def __repr__(self):
        return repr(self.map.values())
        
class HistoryArraySet(UserList.UserList):
    """extends a UserList to provide unique-set functionality as well as history-aware 
    functionality, including information about what list elements were modified 
    and commit/rollback capability.  When a HistoryArraySet is created with or
    without initial data, it is in a "committed" state.  as soon as changes are made
    to the list via the normal list-based access, it tracks "added" and "deleted" items,
    which remain until the history is committed or rolled back."""
    def __init__(self, data = None, readonly=False):
        # stores the array's items as keys, and a value of True, False or None indicating
        # added, deleted, or unchanged for that item
        self.records = OrderedDict()
        if data is not None:
            self.data = data
            for item in data:
                # add items without triggering any change events
                # *ASSUME* the list is unique already.  might want to change this.
                self.records[item] = None
        else:
            self.data = []
        self.readonly=readonly
    def __getattr__(self, attr):
        """proxies unknown HistoryArraySet methods and attributes to the underlying
        data array.  this allows custom list classes to be used."""
        return getattr(self.data, attr)
    def set_data(self, data):
        """sets the data for this HistoryArraySet to be that of the given data.
        duplicates in the incoming list will be removed."""
        # first mark everything current as "deleted"
        for i in self.data:
            self.records[i] = False
            
        # switch array
        self.data = data

        # TODO: fix this up, remove items from array while iterating
        for i in range(0, len(self.data)):
            if not self._setrecord(self.data[i]):
               del self.data[i]
               i -= 1
    def history_contains(self, obj):
        """returns true if the given object exists within the history
        for this HistoryArrayList."""
        return self.records.has_key(obj)
    def __hash__(self):
        return id(self)
    def _setrecord(self, item):
        if self.readonly:
            raise InvalidRequestError("This list is read only")
        try:
            val = self.records[item]
            if val is True or val is None:
                return False
            else:
                self.records[item] = None
                return True
        except KeyError:
            self.records[item] = True
            return True
    def _delrecord(self, item):
        if self.readonly:
            raise InvalidRequestError("This list is read only")
        try:
            val = self.records[item]
            if val is None:
                self.records[item] = False
                return True
            elif val is True:
                del self.records[item]
                return True
            return False
        except KeyError:
            return False
    def commit(self):
        """commits the added values in this list to be the new "unchanged" values.
        values that have been marked as deleted are removed from the history."""
        for key in self.records.keys():
            value = self.records[key]
            if value is False:
                del self.records[key]
            else:
                self.records[key] = None
    def rollback(self):
        """rolls back changes to this list to the last "committed" state."""
        # TODO: speed this up
        list = []
        for key, status in self.records.iteritems():
            if status is False or status is None:
                list.append(key)
        self._clear_data()
        self.records = {}
        for l in list:
            self.append_nohistory(l)
    def clear(self):
        """clears the list and removes all history."""
        self._clear_data()
        self.records = {}
    def _clear_data(self):
        if isinstance(self.data, dict):
            self.data.clear()
        else:
            self.data[:] = []
    def added_items(self):
        """returns a list of items that have been added since the last "committed" state."""
        return [key for key in self.data if self.records[key] is True]
    def deleted_items(self):
        """returns a list of items that have been deleted since the last "committed" state."""
        return [key for key, value in self.records.iteritems() if value is False]
    def unchanged_items(self):
        """returns a list of items that have not been changed since the last "committed" state."""
        return [key for key in self.data if self.records[key] is None]
    def append_nohistory(self, item):
        """appends an item to the list without affecting the "history"."""
        if not self.records.has_key(item):
            self.records[item] = None
            self.data.append(item)
    def remove_nohistory(self, item):
        """removes an item from the list without affecting the "history"."""
        if self.records.has_key(item):
            del self.records[item]
            self.data.remove(item)
    def has_item(self, item):
        return self.records.has_key(item)
    def __setitem__(self, i, item): 
        if self._setrecord(a):
            self.data[i] = item
    def __delitem__(self, i):
        self._delrecord(self.data[i])
        del self.data[i]
    def __setslice__(self, i, j, other):
        i = max(i, 0); j = max(j, 0)
        if isinstance(other, UserList.UserList):
            l = other.data
        elif isinstance(other, type(self.data)):
            l = other
        else:
            l = list(other)
        g = [a for a in l if self._setrecord(a)]
        self.data[i:] = g
    def __delslice__(self, i, j):
        i = max(i, 0); j = max(j, 0)
        for a in self.data[i:j]:
            self._delrecord(a)
        del self.data[i:j]
    def append(self, item): 
        if self._setrecord(item):
            self.data.append(item)
    def insert(self, i, item): 
        if self._setrecord(item):
            self.data.insert(i, item)
    def pop(self, i=-1):
        item = self.data[i]
        if self._delrecord(item):
            return self.data.pop(i)
    def remove(self, item): 
        if self._delrecord(item):
            self.data.remove(item)
    def __add__(self, other):
        raise NotImplementedError()
    def __radd__(self, other):
        raise NotImplementedError()
    def __iadd__(self, other):
        raise NotImplementedError()

        
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
    