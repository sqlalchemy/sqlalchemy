# util/compat.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
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
py3k = sys.version_info >= (3, 0)
py2k = not py3k
jython = sys.platform.startswith('java')
pypy = hasattr(sys, 'pypy_version_info')
win32 = sys.platform.startswith('win')
cpython = not pypy and not jython  # TODO: something better for this ?


next = next

if py3k:
    import pickle
else:
    try:
        import pickle as pickle
    except ImportError:
        import pickle

if py3k:
    from inspect import getfullargspec as inspect_getfullargspec
    from urllib.parse import quote_plus, unquote_plus, parse_qsl
    string_types = str,
    binary_type = bytes
    text_type = str
    int_types = int,
    iterbytes = iter

    def b(s):
        return s.encode("latin-1")

    if py32:
        callable = callable
    else:
        def callable(fn):
            return hasattr(fn, '__call__')

    def cmp(a, b):
        return (a > b) - (a < b)

    from functools import reduce

else:
    from inspect import getargspec as inspect_getfullargspec
    from urllib import quote_plus, unquote_plus
    from urlparse import parse_qsl
    string_types = basestring,
    binary_type = str
    text_type = unicode
    int_types = int, long
    def iterbytes(buf):
        return (ord(byte) for byte in buf)

    def b(s):
        return s

    callable = callable
    cmp = cmp
    reduce = reduce


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

import time
if win32 or jython:
    time_func = time.clock
else:
    time_func = time.time

from collections import namedtuple
from operator import attrgetter as dottedgetter


if py3k:
    def reraise(tp, value, tb=None, cause=None):
        if cause is not None:
            value.__cause__ = cause
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value

    def raise_from_cause(exception, exc_info=None):
        if exc_info is None:
            exc_info = sys.exc_info()
        exc_type, exc_value, exc_tb = exc_info
        reraise(type(exception), exception, tb=exc_tb, cause=exc_value)
else:
    exec("def reraise(tp, value, tb=None, cause=None):\n"
            "    raise tp, value, tb\n")

    def raise_from_cause(exception, exc_info=None):
        # not as nice as that of Py3K, but at least preserves
        # the code line where the issue occurred
        if exc_info is None:
            exc_info = sys.exc_info()
        exc_type, exc_value, exc_tb = exc_info
        reraise(type(exception), exception, tb=exc_tb)


def with_metaclass(meta, *bases):
    """Create a base class with a metaclass."""

    return meta("MetaBase", bases, {})
