# util/compat.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Handle Python version/platform incompatibilities."""

import sys

try:
    import threading
except ImportError:
    import dummy_threading as threading

py32 = sys.version_info >= (3, 2)
py3k_warning = getattr(sys, 'py3kwarning', False) or sys.version_info >= (3, 0)
py3k = sys.version_info >= (3, 0)
jython = sys.platform.startswith('java')
pypy = hasattr(sys, 'pypy_version_info')
win32 = sys.platform.startswith('win')
cpython = not pypy and not jython  # TODO: something better for this ?

if py3k_warning:
    set_types = set
elif sys.version_info < (2, 6):
    import sets
    set_types = set, sets.Set
else:
    # 2.6 deprecates sets.Set, but we still need to be able to detect them
    # in user code and as return values from DB-APIs
    ignore = ('ignore', None, DeprecationWarning, None, 0)
    import warnings
    try:
        warnings.filters.insert(0, ignore)
    except Exception:
        import sets
    else:
        import sets
        warnings.filters.remove(ignore)

    set_types = set, sets.Set

if sys.version_info < (2, 6):
    def next(iter):
        return iter.next()
else:
    next = next
if py3k_warning:
    import pickle
else:
    try:
        import cPickle as pickle
    except ImportError:
        import pickle


# a controversial feature, required by MySQLdb currently
def buffer(x):
    return x

# Py2K
buffer = buffer
# end Py2K

try:
    from contextlib import contextmanager
except ImportError:
    def contextmanager(fn):
        return fn

try:
    from functools import update_wrapper
except ImportError:
    def update_wrapper(wrapper, wrapped,
                       assigned=('__doc__', '__module__', '__name__'),
                       updated=('__dict__',)):
        for attr in assigned:
            setattr(wrapper, attr, getattr(wrapped, attr))
        for attr in updated:
            getattr(wrapper, attr).update(getattr(wrapped, attr, ()))
        return wrapper

try:
    from functools import partial
except ImportError:
    def partial(func, *args, **keywords):
        def newfunc(*fargs, **fkeywords):
            newkeywords = keywords.copy()
            newkeywords.update(fkeywords)
            return func(*(args + fargs), **newkeywords)
        return newfunc


if sys.version_info < (2, 6):
    # emits a nasty deprecation warning
    # in newer pythons
    from cgi import parse_qsl
else:
    from urlparse import parse_qsl

# Py3K
#from inspect import getfullargspec as inspect_getfullargspec
# Py2K
from inspect import getargspec as inspect_getfullargspec
# end Py2K

if py3k_warning:
    # they're bringing it back in 3.2.  brilliant !
    def callable(fn):
        return hasattr(fn, '__call__')

    def cmp(a, b):
        return (a > b) - (a < b)

    from functools import reduce
else:
    callable = callable
    cmp = cmp
    reduce = reduce

try:
    from collections import namedtuple
except ImportError:
    def namedtuple(typename, fieldnames):
        def __new__(cls, *values):
            tup = tuple.__new__(cls, values)
            for i, fname in enumerate(fieldnames):
                setattr(tup, fname, tup[i])
            return tup
        tuptype = type(typename, (tuple, ), {'__new__': __new__})
        return tuptype

try:
    from collections import defaultdict
except ImportError:
    class defaultdict(dict):

        def __init__(self, default_factory=None, *a, **kw):
            if (default_factory is not None and
                not hasattr(default_factory, '__call__')):
                raise TypeError('first argument must be callable')
            dict.__init__(self, *a, **kw)
            self.default_factory = default_factory

        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return self.__missing__(key)

        def __missing__(self, key):
            if self.default_factory is None:
                raise KeyError(key)
            self[key] = value = self.default_factory()
            return value

        def __reduce__(self):
            if self.default_factory is None:
                args = tuple()
            else:
                args = self.default_factory,
            return type(self), args, None, None, self.iteritems()

        def copy(self):
            return self.__copy__()

        def __copy__(self):
            return type(self)(self.default_factory, self)

        def __deepcopy__(self, memo):
            import copy
            return type(self)(self.default_factory,
                              copy.deepcopy(self.items()))

        def __repr__(self):
            return 'defaultdict(%s, %s)' % (self.default_factory,
                                            dict.__repr__(self))

try:
    from weakref import WeakSet
except:
    import weakref

    class WeakSet(object):
        """Implement the small subset of set() which SQLAlchemy needs
        here. """
        def __init__(self, values=None):
            self._storage = weakref.WeakKeyDictionary()
            if values is not None:
                self._storage.update((value, None) for value in values)

        def __iter__(self):
            return iter(self._storage)

        def union(self, other):
            return WeakSet(set(self).union(other))

        def add(self, other):
            self._storage[other] = True


# find or create a dict implementation that supports __missing__
class _probe(dict):
    def __missing__(self, key):
        return 1

try:
    try:
        _probe()['missing']
        py25_dict = dict
    except KeyError:
        class py25_dict(dict):
            def __getitem__(self, key):
                try:
                    return dict.__getitem__(self, key)
                except KeyError:
                    try:
                        missing = self.__missing__
                    except AttributeError:
                        raise KeyError(key)
                    else:
                        return missing(key)
finally:
    del _probe


try:
    import hashlib
    _md5 = hashlib.md5
except ImportError:
    import md5
    _md5 = md5.new


def md5_hex(x):
    # Py3K
    #x = x.encode('utf-8')
    m = _md5()
    m.update(x)
    return m.hexdigest()

import time
if win32 or jython:
    time_func = time.clock
else:
    time_func = time.time

if sys.version_info >= (2, 5):
    any = any
else:
    def any(iterator):
        for item in iterator:
            if bool(item):
                return True
        else:
            return False

if sys.version_info >= (2, 5):
    def decode_slice(slc):
        """decode a slice object as sent to __getitem__.

        takes into account the 2.5 __index__() method, basically.

        """
        ret = []
        for x in slc.start, slc.stop, slc.step:
            if hasattr(x, '__index__'):
                x = x.__index__()
            ret.append(x)
        return tuple(ret)
else:
    def decode_slice(slc):
        return (slc.start, slc.stop, slc.step)

if sys.version_info >= (2, 6):
    from operator import attrgetter as dottedgetter
else:
    def dottedgetter(attr):
        def g(obj):
            for name in attr.split("."):
                obj = getattr(obj, name)
            return obj
        return g


import decimal
