# util/_collections.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Collection classes and helpers."""
import collections.abc as collections_abc
import operator
import types
import weakref

from .compat import threading

try:
    from sqlalchemy.cyextension.immutabledict import ImmutableContainer
    from sqlalchemy.cyextension.immutabledict import immutabledict
    from sqlalchemy.cyextension.collections import IdentitySet
    from sqlalchemy.cyextension.collections import OrderedSet
    from sqlalchemy.cyextension.collections import unique_list  # noqa
except ImportError:
    from ._py_collections import immutabledict
    from ._py_collections import IdentitySet
    from ._py_collections import ImmutableContainer
    from ._py_collections import OrderedSet
    from ._py_collections import unique_list  # noqa


EMPTY_SET = frozenset()


def coerce_to_immutabledict(d):
    if not d:
        return EMPTY_DICT
    elif isinstance(d, immutabledict):
        return d
    else:
        return immutabledict(d)


EMPTY_DICT = immutabledict()


class FacadeDict(ImmutableContainer, dict):
    """A dictionary that is not publicly mutable."""

    clear = pop = popitem = setdefault = update = ImmutableContainer._immutable

    def __new__(cls, *args):
        new = dict.__new__(cls)
        return new

    def copy(self):
        raise NotImplementedError(
            "an immutabledict shouldn't need to be copied.  use dict(d) "
            "if you need a mutable dictionary."
        )

    def __reduce__(self):
        return FacadeDict, (dict(self),)

    def _insert_item(self, key, value):
        """insert an item into the dictionary directly."""
        dict.__setitem__(self, key, value)

    def __repr__(self):
        return "FacadeDict(%s)" % dict.__repr__(self)


class Properties:
    """Provide a __getattr__/__setattr__ interface over a dict."""

    __slots__ = ("_data",)

    def __init__(self, data):
        object.__setattr__(self, "_data", data)

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(list(self._data.values()))

    def __dir__(self):
        return dir(super(Properties, self)) + [
            str(k) for k in self._data.keys()
        ]

    def __add__(self, other):
        return list(self) + list(other)

    def __setitem__(self, key, obj):
        self._data[key] = obj

    def __getitem__(self, key):
        return self._data[key]

    def __delitem__(self, key):
        del self._data[key]

    def __setattr__(self, key, obj):
        self._data[key] = obj

    def __getstate__(self):
        return {"_data": self._data}

    def __setstate__(self, state):
        object.__setattr__(self, "_data", state["_data"])

    def __getattr__(self, key):
        try:
            return self._data[key]
        except KeyError:
            raise AttributeError(key)

    def __contains__(self, key):
        return key in self._data

    def as_immutable(self):
        """Return an immutable proxy for this :class:`.Properties`."""

        return ImmutableProperties(self._data)

    def update(self, value):
        self._data.update(value)

    def get(self, key, default=None):
        if key in self:
            return self[key]
        else:
            return default

    def keys(self):
        return list(self._data)

    def values(self):
        return list(self._data.values())

    def items(self):
        return list(self._data.items())

    def has_key(self, key):
        return key in self._data

    def clear(self):
        self._data.clear()


class OrderedProperties(Properties):
    """Provide a __getattr__/__setattr__ interface with an OrderedDict
    as backing store."""

    __slots__ = ()

    def __init__(self):
        Properties.__init__(self, OrderedDict())


class ImmutableProperties(ImmutableContainer, Properties):
    """Provide immutable dict/object attribute to an underlying dictionary."""

    __slots__ = ()


def _ordered_dictionary_sort(d, key=None):
    """Sort an OrderedDict in-place."""

    items = [(k, d[k]) for k in sorted(d, key=key)]

    d.clear()

    d.update(items)


OrderedDict = dict
sort_dictionary = _ordered_dictionary_sort


class WeakSequence:
    def __init__(self, __elements=()):
        # adapted from weakref.WeakKeyDictionary, prevent reference
        # cycles in the collection itself
        def _remove(item, selfref=weakref.ref(self)):
            self = selfref()
            if self is not None:
                self._storage.remove(item)

        self._remove = _remove
        self._storage = [
            weakref.ref(element, _remove) for element in __elements
        ]

    def append(self, item):
        self._storage.append(weakref.ref(item, self._remove))

    def __len__(self):
        return len(self._storage)

    def __iter__(self):
        return (
            obj for obj in (ref() for ref in self._storage) if obj is not None
        )

    def __getitem__(self, index):
        try:
            obj = self._storage[index]
        except KeyError:
            raise IndexError("Index %s out of range" % index)
        else:
            return obj()


class OrderedIdentitySet(IdentitySet):
    def __init__(self, iterable=None):
        IdentitySet.__init__(self)
        self._members = OrderedDict()
        if iterable:
            for o in iterable:
                self.add(o)


class PopulateDict(dict):
    """A dict which populates missing values via a creation function.

    Note the creation function takes a key, unlike
    collections.defaultdict.

    """

    def __init__(self, creator):
        self.creator = creator

    def __missing__(self, key):
        self[key] = val = self.creator(key)
        return val


class WeakPopulateDict(dict):
    """Like PopulateDict, but assumes a self + a method and does not create
    a reference cycle.

    """

    def __init__(self, creator_method):
        self.creator = creator_method.__func__
        weakself = creator_method.__self__
        self.weakself = weakref.ref(weakself)

    def __missing__(self, key):
        self[key] = val = self.creator(self.weakself(), key)
        return val


# Define collections that are capable of storing
# ColumnElement objects as hashable keys/elements.
# At this point, these are mostly historical, things
# used to be more complicated.
column_set = set
column_dict = dict
ordered_column_set = OrderedSet


_getters = PopulateDict(operator.itemgetter)

_property_getters = PopulateDict(
    lambda idx: property(operator.itemgetter(idx))
)


class UniqueAppender:
    """Appends items to a collection ensuring uniqueness.

    Additional appends() of the same object are ignored.  Membership is
    determined by identity (``is a``) not equality (``==``).
    """

    def __init__(self, data, via=None):
        self.data = data
        self._unique = {}
        if via:
            self._data_appender = getattr(data, via)
        elif hasattr(data, "append"):
            self._data_appender = data.append
        elif hasattr(data, "add"):
            self._data_appender = data.add

    def append(self, item):
        id_ = id(item)
        if id_ not in self._unique:
            self._data_appender(item)
            self._unique[id_] = True

    def __iter__(self):
        return iter(self.data)


def coerce_generator_arg(arg):
    if len(arg) == 1 and isinstance(arg[0], types.GeneratorType):
        return list(arg[0])
    else:
        return arg


def to_list(x, default=None):
    if x is None:
        return default
    if not isinstance(x, collections_abc.Iterable) or isinstance(
        x, (str, bytes)
    ):
        return [x]
    elif isinstance(x, list):
        return x
    else:
        return list(x)


def has_intersection(set_, iterable):
    r"""return True if any items of set\_ are present in iterable.

    Goes through special effort to ensure __hash__ is not called
    on items in iterable that don't support it.

    """
    # TODO: optimize, write in C, etc.
    return bool(set_.intersection([i for i in iterable if i.__hash__]))


def to_set(x):
    if x is None:
        return set()
    if not isinstance(x, set):
        return set(to_list(x))
    else:
        return x


def to_column_set(x):
    if x is None:
        return column_set()
    if not isinstance(x, column_set):
        return column_set(to_list(x))
    else:
        return x


def update_copy(d, _new=None, **kw):
    """Copy the given dict and update with the given values."""

    d = d.copy()
    if _new:
        d.update(_new)
    d.update(**kw)
    return d


def flatten_iterator(x):
    """Given an iterator of which further sub-elements may also be
    iterators, flatten the sub-elements into a single iterator.

    """
    for elem in x:
        if not isinstance(elem, str) and hasattr(elem, "__iter__"):
            for y in flatten_iterator(elem):
                yield y
        else:
            yield elem


class LRUCache(dict):
    """Dictionary with 'squishy' removal of least
    recently used items.

    Note that either get() or [] should be used here, but
    generally its not safe to do an "in" check first as the dictionary
    can change subsequent to that call.

    """

    __slots__ = "capacity", "threshold", "size_alert", "_counter", "_mutex"

    def __init__(self, capacity=100, threshold=0.5, size_alert=None):
        self.capacity = capacity
        self.threshold = threshold
        self.size_alert = size_alert
        self._counter = 0
        self._mutex = threading.Lock()

    def _inc_counter(self):
        self._counter += 1
        return self._counter

    def get(self, key, default=None):
        item = dict.get(self, key, default)
        if item is not default:
            item[2] = self._inc_counter()
            return item[1]
        else:
            return default

    def __getitem__(self, key):
        item = dict.__getitem__(self, key)
        item[2] = self._inc_counter()
        return item[1]

    def values(self):
        return [i[1] for i in dict.values(self)]

    def setdefault(self, key, value):
        if key in self:
            return self[key]
        else:
            self[key] = value
            return value

    def __setitem__(self, key, value):
        item = dict.get(self, key)
        if item is None:
            item = [key, value, self._inc_counter()]
            dict.__setitem__(self, key, item)
        else:
            item[1] = value
        self._manage_size()

    @property
    def size_threshold(self):
        return self.capacity + self.capacity * self.threshold

    def _manage_size(self):
        if not self._mutex.acquire(False):
            return
        try:
            size_alert = bool(self.size_alert)
            while len(self) > self.capacity + self.capacity * self.threshold:
                if size_alert:
                    size_alert = False
                    self.size_alert(self)
                by_counter = sorted(
                    dict.values(self), key=operator.itemgetter(2), reverse=True
                )
                for item in by_counter[self.capacity :]:
                    try:
                        del self[item[0]]
                    except KeyError:
                        # deleted elsewhere; skip
                        continue
        finally:
            self._mutex.release()


class ScopedRegistry:
    """A Registry that can store one or multiple instances of a single
    class on the basis of a "scope" function.

    The object implements ``__call__`` as the "getter", so by
    calling ``myregistry()`` the contained object is returned
    for the current scope.

    :param createfunc:
      a callable that returns a new object to be placed in the registry

    :param scopefunc:
      a callable that will return a key to store/retrieve an object.
    """

    def __init__(self, createfunc, scopefunc):
        """Construct a new :class:`.ScopedRegistry`.

        :param createfunc:  A creation function that will generate
          a new value for the current scope, if none is present.

        :param scopefunc:  A function that returns a hashable
          token representing the current scope (such as, current
          thread identifier).

        """
        self.createfunc = createfunc
        self.scopefunc = scopefunc
        self.registry = {}

    def __call__(self):
        key = self.scopefunc()
        try:
            return self.registry[key]
        except KeyError:
            return self.registry.setdefault(key, self.createfunc())

    def has(self):
        """Return True if an object is present in the current scope."""

        return self.scopefunc() in self.registry

    def set(self, obj):
        """Set the value for the current scope."""

        self.registry[self.scopefunc()] = obj

    def clear(self):
        """Clear the current scope, if any."""

        try:
            del self.registry[self.scopefunc()]
        except KeyError:
            pass


class ThreadLocalRegistry(ScopedRegistry):
    """A :class:`.ScopedRegistry` that uses a ``threading.local()``
    variable for storage.

    """

    def __init__(self, createfunc):
        self.createfunc = createfunc
        self.registry = threading.local()

    def __call__(self):
        try:
            return self.registry.value
        except AttributeError:
            val = self.registry.value = self.createfunc()
            return val

    def has(self):
        return hasattr(self.registry, "value")

    def set(self, obj):
        self.registry.value = obj

    def clear(self):
        try:
            del self.registry.value
        except AttributeError:
            pass


def has_dupes(sequence, target):
    """Given a sequence and search object, return True if there's more
    than one, False if zero or one of them.


    """
    # compare to .index version below, this version introduces less function
    # overhead and is usually the same speed.  At 15000 items (way bigger than
    # a relationship-bound collection in memory usually is) it begins to
    # fall behind the other version only by microseconds.
    c = 0
    for item in sequence:
        if item is target:
            c += 1
            if c > 1:
                return True
    return False


# .index version.  the two __contains__ calls as well
# as .index() and isinstance() slow this down.
# def has_dupes(sequence, target):
#    if target not in sequence:
#        return False
#    elif not isinstance(sequence, collections_abc.Sequence):
#        return False
#
#    idx = sequence.index(target)
#    return target in sequence[idx + 1:]
