# util.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import inspect, itertools, operator, sys, warnings, weakref, gc
# Py2K
import __builtin__
# end Py2K
types = __import__('types')

from sqlalchemy import exc

try:
    import threading
except ImportError:
    import dummy_threading as threading

py3k = getattr(sys, 'py3kwarning', False) or sys.version_info >= (3, 0)
jython = sys.platform.startswith('java')
win32 = sys.platform.startswith('win')

if py3k:
    set_types = set
elif sys.version_info < (2, 6):
    import sets
    set_types = set, sets.Set
else:
    # 2.6 deprecates sets.Set, but we still need to be able to detect them
    # in user code and as return values from DB-APIs
    ignore = ('ignore', None, DeprecationWarning, None, 0)
    try:
        warnings.filters.insert(0, ignore)
    except Exception:
        import sets
    else:
        import sets
        warnings.filters.remove(ignore)

    set_types = set, sets.Set

EMPTY_SET = frozenset()

NoneType = type(None)

if py3k:
    import pickle
else:
    try:
        import cPickle as pickle
    except ImportError:
        import pickle

# Py2K
# a controversial feature, required by MySQLdb currently
def buffer(x):
    return x 
    
buffer = getattr(__builtin__, 'buffer', buffer)
# end Py2K
        
if sys.version_info >= (2, 5):
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
else:
    class PopulateDict(dict):
        """A dict which populates missing values via a creation function."""

        def __init__(self, creator):
            self.creator = creator
            
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                self[key] = value = self.creator(key)
                return value

if py3k:
    def callable(fn):
        return hasattr(fn, '__call__')
    def cmp(a, b):
        return (a > b) - (a < b)

    from functools import reduce
else:
    callable = __builtin__.callable
    cmp = __builtin__.cmp
    reduce = __builtin__.reduce

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

class frozendict(dict):
    @property
    def _blocked_attribute(obj):
        raise AttributeError, "A frozendict cannot be modified."

    __delitem__ = __setitem__ = clear = _blocked_attribute
    pop = popitem = setdefault = update = _blocked_attribute

    def __new__(cls, *args):
        new = dict.__new__(cls)
        dict.__init__(new, *args)
        return new

    def __init__(self, *args):
        pass

    def __reduce__(self):
        return frozendict, (dict(self), )

    def union(self, d):
        if not self:
            return frozendict(d)
        else:
            d2 = self.copy()
            d2.update(d)
            return frozendict(d2)
            
    def __repr__(self):
        return "frozendict(%s)" % dict.__repr__(self)

def to_list(x, default=None):
    if x is None:
        return default
    if not isinstance(x, (list, tuple)):
        return [x]
    else:
        return x

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
except:
    def partial(func, *args, **keywords):
        def newfunc(*fargs, **fkeywords):
            newkeywords = keywords.copy()
            newkeywords.update(fkeywords)
            return func(*(args + fargs), **newkeywords)
        return newfunc


def accepts_a_list_as_starargs(list_deprecation=None):
    def decorate(fn):

        spec = inspect.getargspec(fn)
        assert spec[1], 'Decorated function does not accept *args'

        def _deprecate():
            if list_deprecation:
                if list_deprecation == 'pending':
                    warning_type = exc.SAPendingDeprecationWarning
                else:
                    warning_type = exc.SADeprecationWarning
                msg = (
                    "%s%s now accepts multiple %s arguments as a "
                    "variable argument list.  Supplying %s as a single "
                    "list is deprecated and support will be removed "
                    "in a future release." % (
                        fn.func_name,
                        inspect.formatargspec(*spec),
                        spec[1], spec[1]))
                warnings.warn(msg, warning_type, stacklevel=3)

        def go(fn, *args, **kw): 
            if isinstance(args[-1], list): 
                _deprecate() 
                return fn(*(list(args[0:-1]) + args[-1]), **kw)
            else: 
                return fn(*args, **kw) 
         
        return decorator(go)(fn)

    return decorate

def unique_symbols(used, *bases):
    used = set(used)
    for base in bases:
        pool = itertools.chain((base,),
                               itertools.imap(lambda i: base + str(i),
                                              xrange(1000)))
        for sym in pool:
            if sym not in used:
                used.add(sym)
                yield sym
                break
        else:
            raise NameError("exhausted namespace for symbol base %s" % base)

def decorator(target):
    """A signature-matching decorator factory."""

    def decorate(fn):
        spec = inspect.getargspec(fn)
        names = tuple(spec[0]) + spec[1:3] + (fn.func_name,)
        targ_name, fn_name = unique_symbols(names, 'target', 'fn')

        metadata = dict(target=targ_name, fn=fn_name)
        metadata.update(format_argspec_plus(spec, grouped=False))

        code = 'lambda %(args)s: %(target)s(%(fn)s, %(apply_kw)s)' % (
                metadata)
        decorated = eval(code, {targ_name:target, fn_name:fn})
        decorated.func_defaults = getattr(fn, 'im_func', fn).func_defaults
        return update_wrapper(decorated, fn)
    return update_wrapper(decorate, target)


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
        if not isinstance(elem, basestring) and hasattr(elem, '__iter__'):
            for y in flatten_iterator(elem):
                yield y
        else:
            yield elem

def get_cls_kwargs(cls):
    """Return the full set of inherited kwargs for the given `cls`.

    Probes a class's __init__ method, collecting all named arguments.  If the
    __init__ defines a \**kwargs catch-all, then the constructor is presumed to
    pass along unrecognized keywords to it's base classes, and the collection
    process is repeated recursively on each of the bases.
    
    """

    for c in cls.__mro__:
        if '__init__' in c.__dict__:
            stack = set([c])
            break
    else:
        return []

    args = set()
    while stack:
        class_ = stack.pop()
        ctr = class_.__dict__.get('__init__', False)
        if not ctr or not isinstance(ctr, types.FunctionType):
            stack.update(class_.__bases__)
            continue
        names, _, has_kw, _ = inspect.getargspec(ctr)
        args.update(names)
        if has_kw:
            stack.update(class_.__bases__)
    args.discard('self')
    return args

def get_func_kwargs(func):
    """Return the full set of legal kwargs for the given `func`."""
    return inspect.getargspec(func)[0]

def format_argspec_plus(fn, grouped=True):
    """Returns a dictionary of formatted, introspected function arguments.

    A enhanced variant of inspect.formatargspec to support code generation.

    fn
       An inspectable callable or tuple of inspect getargspec() results.
    grouped
      Defaults to True; include (parens, around, argument) lists

    Returns:

    args
      Full inspect.formatargspec for fn
    self_arg
      The name of the first positional argument, varargs[0], or None
      if the function defines no positional arguments.
    apply_pos
      args, re-written in calling rather than receiving syntax.  Arguments are
      passed positionally.
    apply_kw
      Like apply_pos, except keyword-ish args are passed as keywords.

    Example::

      >>> format_argspec_plus(lambda self, a, b, c=3, **d: 123)
      {'args': '(self, a, b, c=3, **d)',
       'self_arg': 'self',
       'apply_kw': '(self, a, b, c=c, **d)',
       'apply_pos': '(self, a, b, c, **d)'}

    """
    spec = callable(fn) and inspect.getargspec(fn) or fn
    args = inspect.formatargspec(*spec)
    if spec[0]:
        self_arg = spec[0][0]
    elif spec[1]:
        self_arg = '%s[0]' % spec[1]
    else:
        self_arg = None
    apply_pos = inspect.formatargspec(spec[0], spec[1], spec[2])
    defaulted_vals = spec[3] is not None and spec[0][0-len(spec[3]):] or ()
    apply_kw = inspect.formatargspec(spec[0], spec[1], spec[2], defaulted_vals,
                                     formatvalue=lambda x: '=' + x)
    if grouped:
        return dict(args=args, self_arg=self_arg,
                    apply_pos=apply_pos, apply_kw=apply_kw)
    else:
        return dict(args=args[1:-1], self_arg=self_arg,
                    apply_pos=apply_pos[1:-1], apply_kw=apply_kw[1:-1])

def format_argspec_init(method, grouped=True):
    """format_argspec_plus with considerations for typical __init__ methods

    Wraps format_argspec_plus with error handling strategies for typical
    __init__ cases::

      object.__init__ -> (self)
      other unreflectable (usually C) -> (self, *args, **kwargs)

    """
    try:
        return format_argspec_plus(method, grouped=grouped)
    except TypeError:
        self_arg = 'self'
        if method is object.__init__:
            args = grouped and '(self)' or 'self'
        else:
            args = (grouped and '(self, *args, **kwargs)'
                            or 'self, *args, **kwargs')
        return dict(self_arg='self', args=args, apply_pos=args, apply_kw=args)

def getargspec_init(method):
    """inspect.getargspec with considerations for typical __init__ methods

    Wraps inspect.getargspec with error handling for typical __init__ cases::

      object.__init__ -> (self)
      other unreflectable (usually C) -> (self, *args, **kwargs)

    """
    try:
        return inspect.getargspec(method)
    except TypeError:
        if method is object.__init__:
            return (['self'], None, None, None)
        else:
            return (['self'], 'args', 'kwargs', None)

    
def unbound_method_to_callable(func_or_cls):
    """Adjust the incoming callable such that a 'self' argument is not required."""

    if isinstance(func_or_cls, types.MethodType) and not func_or_cls.im_self:
        return func_or_cls.im_func
    else:
        return func_or_cls

class portable_instancemethod(object):
    """Turn an instancemethod into a (parent, name) pair
    to produce a serializable callable.
    
    """
    def __init__(self, meth):
        self.target = meth.im_self
        self.name = meth.__name__

    def __call__(self, *arg, **kw):
        return getattr(self.target, self.name)(*arg, **kw)
        
def class_hierarchy(cls):
    """Return an unordered sequence of all classes related to cls.

    Traverses diamond hierarchies.

    Fibs slightly: subclasses of builtin types are not returned.  Thus
    class_hierarchy(class A(object)) returns (A, object), not A plus every
    class systemwide that derives from object.

    Old-style classes are discarded and hierarchies rooted on them
    will not be descended.

    """
    # Py2K
    if isinstance(cls, types.ClassType):
        return list()
    # end Py2K
    hier = set([cls])
    process = list(cls.__mro__)
    while process:
        c = process.pop()
        # Py2K
        if isinstance(c, types.ClassType):
            continue
        for b in (_ for _ in c.__bases__
                  if _ not in hier and not isinstance(_, types.ClassType)):
        # end Py2K
        # Py3K
        #for b in (_ for _ in c.__bases__
        #          if _ not in hier):
            process.append(b)
            hier.add(b)
        # Py3K
        #if c.__module__ == 'builtins' or not hasattr(c, '__subclasses__'):
        #    continue
        # Py2K
        if c.__module__ == '__builtin__' or not hasattr(c, '__subclasses__'):
            continue
        # end Py2K
        for s in [_ for _ in c.__subclasses__() if _ not in hier]:
            process.append(s)
            hier.add(s)
    return list(hier)

def iterate_attributes(cls):
    """iterate all the keys and attributes associated 
       with a class, without using getattr().

       Does not use getattr() so that class-sensitive 
       descriptors (i.e. property.__get__()) are not called.

    """
    keys = dir(cls)
    for key in keys:
        for c in cls.__mro__:
            if key in c.__dict__:
                yield (key, c.__dict__[key])
                break

# from paste.deploy.converters
def asbool(obj):
    if isinstance(obj, (str, unicode)):
        obj = obj.strip().lower()
        if obj in ['true', 'yes', 'on', 'y', 't', '1']:
            return True
        elif obj in ['false', 'no', 'off', 'n', 'f', '0']:
            return False
        else:
            raise ValueError("String is not true/false: %r" % obj)
    return bool(obj)

def coerce_kw_type(kw, key, type_, flexi_bool=True):
    """If 'key' is present in dict 'kw', coerce its value to type 'type\_' if
    necessary.  If 'flexi_bool' is True, the string '0' is considered false
    when coercing to boolean.
    """

    if key in kw and type(kw[key]) is not type_ and kw[key] is not None:
        if type_ is bool and flexi_bool:
            kw[key] = asbool(kw[key])
        else:
            kw[key] = type_(kw[key])

def duck_type_collection(specimen, default=None):
    """Given an instance or class, guess if it is or is acting as one of
    the basic collection types: list, set and dict.  If the __emulates__
    property is present, return that preferentially.
    """

    if hasattr(specimen, '__emulates__'):
        # canonicalize set vs sets.Set to a standard: the builtin set
        if (specimen.__emulates__ is not None and
            issubclass(specimen.__emulates__, set_types)):
            return set
        else:
            return specimen.__emulates__

    isa = isinstance(specimen, type) and issubclass or isinstance
    if isa(specimen, list):
        return list
    elif isa(specimen, set_types):
        return set
    elif isa(specimen, dict):
        return dict

    if hasattr(specimen, 'append'):
        return list
    elif hasattr(specimen, 'add'):
        return set
    elif hasattr(specimen, 'set'):
        return dict
    else:
        return default

def dictlike_iteritems(dictlike):
    """Return a (key, value) iterator for almost any dict-like object."""

    # Py3K
    #if hasattr(dictlike, 'items'):
    #    return dictlike.items()
    # Py2K
    if hasattr(dictlike, 'iteritems'):
        return dictlike.iteritems()
    elif hasattr(dictlike, 'items'):
        return iter(dictlike.items())
    # end Py2K

    getter = getattr(dictlike, '__getitem__', getattr(dictlike, 'get', None))
    if getter is None:
        raise TypeError(
            "Object '%r' is not dict-like" % dictlike)

    if hasattr(dictlike, 'iterkeys'):
        def iterator():
            for key in dictlike.iterkeys():
                yield key, getter(key)
        return iterator()
    elif hasattr(dictlike, 'keys'):
        return iter((key, getter(key)) for key in dictlike.keys())
    else:
        raise TypeError(
            "Object '%r' is not dict-like" % dictlike)

def assert_arg_type(arg, argtype, name):
    if isinstance(arg, argtype):
        return arg
    else:
        if isinstance(argtype, tuple):
            raise exc.ArgumentError(
                            "Argument '%s' is expected to be one of type %s, got '%s'" % 
                            (name, ' or '.join("'%s'" % a for a in argtype), type(arg)))
        else:
            raise exc.ArgumentError(
                            "Argument '%s' is expected to be of type '%s', got '%s'" % 
                            (name, argtype, type(arg)))

_creation_order = 1
def set_creation_order(instance):
    """Assign a '_creation_order' sequence to the given instance.

    This allows multiple instances to be sorted in order of creation
    (typically within a single thread; the counter is not particularly
    threadsafe).

    """
    global _creation_order
    instance._creation_order = _creation_order
    _creation_order +=1

def warn_exception(func, *args, **kwargs):
    """executes the given function, catches all exceptions and converts to a warning."""
    try:
        return func(*args, **kwargs)
    except:
        warn("%s('%s') ignored" % sys.exc_info()[0:2])

def monkeypatch_proxied_specials(into_cls, from_cls, skip=None, only=None,
                                 name='self.proxy', from_instance=None):
    """Automates delegation of __specials__ for a proxying type."""

    if only:
        dunders = only
    else:
        if skip is None:
            skip = ('__slots__', '__del__', '__getattribute__',
                    '__metaclass__', '__getstate__', '__setstate__')
        dunders = [m for m in dir(from_cls)
                   if (m.startswith('__') and m.endswith('__') and
                       not hasattr(into_cls, m) and m not in skip)]
    for method in dunders:
        try:
            fn = getattr(from_cls, method)
            if not hasattr(fn, '__call__'):
                continue
            fn = getattr(fn, 'im_func', fn)
        except AttributeError:
            continue
        try:
            spec = inspect.getargspec(fn)
            fn_args = inspect.formatargspec(spec[0])
            d_args = inspect.formatargspec(spec[0][1:])
        except TypeError:
            fn_args = '(self, *args, **kw)'
            d_args = '(*args, **kw)'

        py = ("def %(method)s%(fn_args)s: "
              "return %(name)s.%(method)s%(d_args)s" % locals())

        env = from_instance is not None and {name: from_instance} or {}
        exec py in env
        try:
            env[method].func_defaults = fn.func_defaults
        except AttributeError:
            pass
        setattr(into_cls, method, env[method])

class NamedTuple(tuple):
    """tuple() subclass that adds labeled names.
    
    Is also pickleable.
    
    """

    def __new__(cls, vals, labels=None):
        vals = list(vals)
        t = tuple.__new__(cls, vals)
        if labels:
            t.__dict__ = dict(itertools.izip(labels, vals))
            t._labels = labels
        return t

    def keys(self):
        return self._labels


class OrderedProperties(object):
    """An object that maintains the order in which attributes are set upon it.

    Also provides an iterator and a very basic getitem/setitem
    interface to those attributes.

    (Not really a dict, since it iterates over values, not keys.  Not really
    a list, either, since each value must have a key associated; hence there is
    no append or extend.)
    """

    def __init__(self):
        self.__dict__['_data'] = OrderedDict()

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return self._data.itervalues()

    def __add__(self, other):
        return list(self) + list(other)

    def __setitem__(self, key, object):
        self._data[key] = object

    def __getitem__(self, key):
        return self._data[key]

    def __delitem__(self, key):
        del self._data[key]

    def __setattr__(self, key, object):
        self._data[key] = object

    def __getstate__(self):
        return {'_data': self.__dict__['_data']}

    def __setstate__(self, state):
        self.__dict__['_data'] = state['_data']

    def __getattr__(self, key):
        try:
            return self._data[key]
        except KeyError:
            raise AttributeError(key)

    def __contains__(self, key):
        return key in self._data

    def update(self, value):
        self._data.update(value)

    def get(self, key, default=None):
        if key in self:
            return self[key]
        else:
            return default

    def keys(self):
        return self._data.keys()

    def has_key(self, key):
        return key in self._data

    def clear(self):
        self._data.clear()

class OrderedDict(dict):
    """A dict that returns keys/values/items in the order they were added."""

    def __init__(self, ____sequence=None, **kwargs):
        self._list = []
        if ____sequence is None:
            if kwargs:
                self.update(**kwargs)
        else:
            self.update(____sequence, **kwargs)

    def clear(self):
        self._list = []
        dict.clear(self)

    def copy(self):
        return self.__copy__()

    def __copy__(self):
        return OrderedDict(self)

    def sort(self, *arg, **kw):
        self._list.sort(*arg, **kw)

    def update(self, ____sequence=None, **kwargs):
        if ____sequence is not None:
            if hasattr(____sequence, 'keys'):
                for key in ____sequence.keys():
                    self.__setitem__(key, ____sequence[key])
            else:
                for key, value in ____sequence:
                    self[key] = value
        if kwargs:
            self.update(kwargs)

    def setdefault(self, key, value):
        if key not in self:
            self.__setitem__(key, value)
            return value
        else:
            return self.__getitem__(key)

    def __iter__(self):
        return iter(self._list)

    def values(self):
        return [self[key] for key in self._list]

    def itervalues(self):
        return iter(self.values())

    def keys(self):
        return list(self._list)

    def iterkeys(self):
        return iter(self.keys())

    def items(self):
        return [(key, self[key]) for key in self.keys()]

    def iteritems(self):
        return iter(self.items())

    def __setitem__(self, key, object):
        if key not in self:
            try:
                self._list.append(key)
            except AttributeError:
                # work around Python pickle loads() with 
                # dict subclass (seems to ignore __setstate__?)
                self._list = [key]
        dict.__setitem__(self, key, object)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self._list.remove(key)

    def pop(self, key, *default):
        present = key in self
        value = dict.pop(self, key, *default)
        if present:
            self._list.remove(key)
        return value

    def popitem(self):
        item = dict.popitem(self)
        self._list.remove(item[0])
        return item

class OrderedSet(set):
    def __init__(self, d=None):
        set.__init__(self)
        self._list = []
        if d is not None:
            self.update(d)

    def add(self, element):
        if element not in self:
            self._list.append(element)
        set.add(self, element)

    def remove(self, element):
        set.remove(self, element)
        self._list.remove(element)

    def insert(self, pos, element):
        if element not in self:
            self._list.insert(pos, element)
        set.add(self, element)

    def discard(self, element):
        if element in self:
            self._list.remove(element)
            set.remove(self, element)

    def clear(self):
        set.clear(self)
        self._list = []

    def __getitem__(self, key):
        return self._list[key]

    def __iter__(self):
        return iter(self._list)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self._list)

    __str__ = __repr__

    def update(self, iterable):
        add = self.add
        for i in iterable:
            add(i)
        return self

    __ior__ = update

    def union(self, other):
        result = self.__class__(self)
        result.update(other)
        return result

    __or__ = union

    def intersection(self, other):
        other = set(other)
        return self.__class__(a for a in self if a in other)

    __and__ = intersection

    def symmetric_difference(self, other):
        other = set(other)
        result = self.__class__(a for a in self if a not in other)
        result.update(a for a in other if a not in self)
        return result

    __xor__ = symmetric_difference

    def difference(self, other):
        other = set(other)
        return self.__class__(a for a in self if a not in other)

    __sub__ = difference

    def intersection_update(self, other):
        other = set(other)
        set.intersection_update(self, other)
        self._list = [ a for a in self._list if a in other]
        return self

    __iand__ = intersection_update

    def symmetric_difference_update(self, other):
        set.symmetric_difference_update(self, other)
        self._list =  [ a for a in self._list if a in self]
        self._list += [ a for a in other._list if a in self]
        return self

    __ixor__ = symmetric_difference_update

    def difference_update(self, other):
        set.difference_update(self, other)
        self._list = [ a for a in self._list if a in self]
        return self

    __isub__ = difference_update


class IdentitySet(object):
    """A set that considers only object id() for uniqueness.

    This strategy has edge cases for builtin types- it's possible to have
    two 'foo' strings in one of these sets, for example.  Use sparingly.
    
    """

    _working_set = set
    
    def __init__(self, iterable=None):
        self._members = dict()
        if iterable:
            for o in iterable:
                self.add(o)

    def add(self, value):
        self._members[id(value)] = value

    def __contains__(self, value):
        return id(value) in self._members

    def remove(self, value):
        del self._members[id(value)]

    def discard(self, value):
        try:
            self.remove(value)
        except KeyError:
            pass

    def pop(self):
        try:
            pair = self._members.popitem()
            return pair[1]
        except KeyError:
            raise KeyError('pop from an empty set')

    def clear(self):
        self._members.clear()

    def __cmp__(self, other):
        raise TypeError('cannot compare sets using cmp()')

    def __eq__(self, other):
        if isinstance(other, IdentitySet):
            return self._members == other._members
        else:
            return False

    def __ne__(self, other):
        if isinstance(other, IdentitySet):
            return self._members != other._members
        else:
            return True

    def issubset(self, iterable):
        other = type(self)(iterable)

        if len(self) > len(other):
            return False
        for m in itertools.ifilterfalse(other._members.__contains__,
                                        self._members.iterkeys()):
            return False
        return True

    def __le__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.issubset(other)

    def __lt__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return len(self) < len(other) and self.issubset(other)

    def issuperset(self, iterable):
        other = type(self)(iterable)

        if len(self) < len(other):
            return False

        for m in itertools.ifilterfalse(self._members.__contains__,
                                        other._members.iterkeys()):
            return False
        return True

    def __ge__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.issuperset(other)

    def __gt__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return len(self) > len(other) and self.issuperset(other)

    def union(self, iterable):
        result = type(self)()
        # testlib.pragma exempt:__hash__
        result._members.update(
            self._working_set(self._member_id_tuples()).union(_iter_id(iterable)))
        return result

    def __or__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.union(other)

    def update(self, iterable):
        self._members = self.union(iterable)._members

    def __ior__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.update(other)
        return self

    def difference(self, iterable):
        result = type(self)()
        # testlib.pragma exempt:__hash__
        result._members.update(
            self._working_set(self._member_id_tuples()).difference(_iter_id(iterable)))
        return result

    def __sub__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.difference(other)

    def difference_update(self, iterable):
        self._members = self.difference(iterable)._members

    def __isub__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.difference_update(other)
        return self

    def intersection(self, iterable):
        result = type(self)()
        # testlib.pragma exempt:__hash__
        result._members.update(
            self._working_set(self._member_id_tuples()).intersection(_iter_id(iterable)))
        return result

    def __and__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.intersection(other)

    def intersection_update(self, iterable):
        self._members = self.intersection(iterable)._members

    def __iand__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.intersection_update(other)
        return self

    def symmetric_difference(self, iterable):
        result = type(self)()
        # testlib.pragma exempt:__hash__
        result._members.update(
            self._working_set(self._member_id_tuples()).symmetric_difference(_iter_id(iterable)))
        return result
    
    def _member_id_tuples(self):
        return ((id(v), v) for v in self._members.itervalues())
        
    def __xor__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        return self.symmetric_difference(other)

    def symmetric_difference_update(self, iterable):
        self._members = self.symmetric_difference(iterable)._members

    def __ixor__(self, other):
        if not isinstance(other, IdentitySet):
            return NotImplemented
        self.symmetric_difference(other)
        return self

    def copy(self):
        return type(self)(self._members.itervalues())

    __copy__ = copy

    def __len__(self):
        return len(self._members)

    def __iter__(self):
        return self._members.itervalues()

    def __hash__(self):
        raise TypeError('set objects are unhashable')

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self._members.values())


class OrderedIdentitySet(IdentitySet):
    class _working_set(OrderedSet):
        # a testing pragma: exempt the OIDS working set from the test suite's
        # "never call the user's __hash__" assertions.  this is a big hammer,
        # but it's safe here: IDS operates on (id, instance) tuples in the
        # working set.
        __sa_hash_exempt__ = True
    
    def __init__(self, iterable=None):
        IdentitySet.__init__(self)
        self._members = OrderedDict()
        if iterable:
            for o in iterable:
                self.add(o)

def _iter_id(iterable):
    """Generator: ((id(o), o) for o in iterable)."""

    for item in iterable:
        yield id(item), item

# define collections that are capable of storing 
# ColumnElement objects as hashable keys/elements.
column_set = set
column_dict = dict
ordered_column_set = OrderedSet
populate_column_dict = PopulateDict

def unique_list(seq, compare_with=set):
    seen = compare_with()
    return [x for x in seq if x not in seen and not seen.add(x)]    

class UniqueAppender(object):
    """Appends items to a collection ensuring uniqueness.

    Additional appends() of the same object are ignored.  Membership is
    determined by identity (``is a``) not equality (``==``).
    """

    def __init__(self, data, via=None):
        self.data = data
        self._unique = IdentitySet()
        if via:
            self._data_appender = getattr(data, via)
        elif hasattr(data, 'append'):
            self._data_appender = data.append
        elif hasattr(data, 'add'):
            # TODO: we think its a set here.  bypass unneeded uniquing logic ?
            self._data_appender = data.add

    def append(self, item):
        if item not in self._unique:
            self._data_appender(item)
            self._unique.add(item)

    def __iter__(self):
        return iter(self.data)


class ScopedRegistry(object):
    """A Registry that can store one or multiple instances of a single
    class on a per-thread scoped basis, or on a customized scope.

    createfunc
      a callable that returns a new object to be placed in the registry

    scopefunc
      a callable that will return a key to store/retrieve an object.
    """

    def __init__(self, createfunc, scopefunc):
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
        return self.scopefunc() in self.registry

    def set(self, obj):
        self.registry[self.scopefunc()] = obj

    def clear(self):
        try:
            del self.registry[self.scopefunc()]
        except KeyError:
            pass

class ThreadLocalRegistry(ScopedRegistry):
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

class _symbol(object):
    def __init__(self, name):
        """Construct a new named symbol."""
        assert isinstance(name, str)
        self.name = name
    def __reduce__(self):
        return symbol, (self.name,)
    def __repr__(self):
        return "<symbol '%s>" % self.name
_symbol.__name__ = 'symbol'


class symbol(object):
    """A constant symbol.

    >>> symbol('foo') is symbol('foo')
    True
    >>> symbol('foo')
    <symbol 'foo>

    A slight refinement of the MAGICCOOKIE=object() pattern.  The primary
    advantage of symbol() is its repr().  They are also singletons.

    Repeated calls of symbol('name') will all return the same instance.

    """
    symbols = {}
    _lock = threading.Lock()

    def __new__(cls, name):
        cls._lock.acquire()
        try:
            sym = cls.symbols.get(name)
            if sym is None:
                cls.symbols[name] = sym = _symbol(name)
            return sym
        finally:
            symbol._lock.release()


def as_interface(obj, cls=None, methods=None, required=None):
    """Ensure basic interface compliance for an instance or dict of callables.

    Checks that ``obj`` implements public methods of ``cls`` or has members
    listed in ``methods``.  If ``required`` is not supplied, implementing at
    least one interface method is sufficient.  Methods present on ``obj`` that
    are not in the interface are ignored.

    If ``obj`` is a dict and ``dict`` does not meet the interface
    requirements, the keys of the dictionary are inspected. Keys present in
    ``obj`` that are not in the interface will raise TypeErrors.

    Raises TypeError if ``obj`` does not meet the interface criteria.

    In all passing cases, an object with callable members is returned.  In the
    simple case, ``obj`` is returned as-is; if dict processing kicks in then
    an anonymous class is returned.

    obj
      A type, instance, or dictionary of callables.
    cls
      Optional, a type.  All public methods of cls are considered the
      interface.  An ``obj`` instance of cls will always pass, ignoring
      ``required``..
    methods
      Optional, a sequence of method names to consider as the interface.
    required
      Optional, a sequence of mandatory implementations. If omitted, an
      ``obj`` that provides at least one interface method is considered
      sufficient.  As a convenience, required may be a type, in which case
      all public methods of the type are required.

    """
    if not cls and not methods:
        raise TypeError('a class or collection of method names are required')

    if isinstance(cls, type) and isinstance(obj, cls):
        return obj

    interface = set(methods or [m for m in dir(cls) if not m.startswith('_')])
    implemented = set(dir(obj))

    complies = operator.ge
    if isinstance(required, type):
        required = interface
    elif not required:
        required = set()
        complies = operator.gt
    else:
        required = set(required)

    if complies(implemented.intersection(interface), required):
        return obj

    # No dict duck typing here.
    if not type(obj) is dict:
        qualifier = complies is operator.gt and 'any of' or 'all of'
        raise TypeError("%r does not implement %s: %s" % (
            obj, qualifier, ', '.join(interface)))

    class AnonymousInterface(object):
        """A callable-holding shell."""

    if cls:
        AnonymousInterface.__name__ = 'Anonymous' + cls.__name__
    found = set()

    for method, impl in dictlike_iteritems(obj):
        if method not in interface:
            raise TypeError("%r: unknown in this interface" % method)
        if not callable(impl):
            raise TypeError("%r=%r is not callable" % (method, impl))
        setattr(AnonymousInterface, method, staticmethod(impl))
        found.add(method)

    if complies(found, required):
        return AnonymousInterface

    raise TypeError("dictionary does not contain required keys %s" %
                    ', '.join(required - found))

def function_named(fn, name):
    """Return a function with a given __name__.

    Will assign to __name__ and return the original function if possible on
    the Python implementation, otherwise a new function will be constructed.

    """
    try:
        fn.__name__ = name
    except TypeError:
        fn = types.FunctionType(fn.func_code, fn.func_globals, name,
                          fn.func_defaults, fn.func_closure)
    return fn

class memoized_property(object):
    """A read-only @property that is only evaluated once."""
    def __init__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__[self.__name__] = result = self.fget(obj)
        return result


class memoized_instancemethod(object):
    """Decorate a method memoize its return value.

    Best applied to no-arg methods: memoization is not sensitive to
    argument values, and will always return the same value even when
    called with different arguments.

    """
    def __init__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return self
        def oneshot(*args, **kw):
            result = self.fget(obj, *args, **kw)
            memo = lambda *a, **kw: result
            memo.__name__ = self.__name__
            memo.__doc__ = self.__doc__
            obj.__dict__[self.__name__] = memo
            return result
        oneshot.__name__ = self.__name__
        oneshot.__doc__ = self.__doc__
        return oneshot

def reset_memoized(instance, name):
    instance.__dict__.pop(name, None)

class WeakIdentityMapping(weakref.WeakKeyDictionary):
    """A WeakKeyDictionary with an object identity index.

    Adds a .by_id dictionary to a regular WeakKeyDictionary.  Trades
    performance during mutation operations for accelerated lookups by id().

    The usual cautions about weak dictionaries and iteration also apply to
    this subclass.

    """
    _none = symbol('none')

    def __init__(self):
        weakref.WeakKeyDictionary.__init__(self)
        self.by_id = {}
        self._weakrefs = {}

    def __setitem__(self, object, value):
        oid = id(object)
        self.by_id[oid] = value
        if oid not in self._weakrefs:
            self._weakrefs[oid] = self._ref(object)
        weakref.WeakKeyDictionary.__setitem__(self, object, value)

    def __delitem__(self, object):
        del self._weakrefs[id(object)]
        del self.by_id[id(object)]
        weakref.WeakKeyDictionary.__delitem__(self, object)

    def setdefault(self, object, default=None):
        value = weakref.WeakKeyDictionary.setdefault(self, object, default)
        oid = id(object)
        if value is default:
            self.by_id[oid] = default
        if oid not in self._weakrefs:
            self._weakrefs[oid] = self._ref(object)
        return value

    def pop(self, object, default=_none):
        if default is self._none:
            value = weakref.WeakKeyDictionary.pop(self, object)
        else:
            value = weakref.WeakKeyDictionary.pop(self, object, default)
        if id(object) in self.by_id:
            del self._weakrefs[id(object)]
            del self.by_id[id(object)]
        return value

    def popitem(self):
        item = weakref.WeakKeyDictionary.popitem(self)
        oid = id(item[0])
        del self._weakrefs[oid]
        del self.by_id[oid]
        return item

    def clear(self):
        # Py2K
        # in 3k, MutableMapping calls popitem()
        self._weakrefs.clear()
        self.by_id.clear()
        # end Py2K
        weakref.WeakKeyDictionary.clear(self)

    def update(self, *a, **kw):
        raise NotImplementedError

    def _cleanup(self, wr, key=None):
        if key is None:
            key = wr.key
        try:
            del self._weakrefs[key]
        except (KeyError, AttributeError):  # pragma: no cover
            pass                            # pragma: no cover
        try:
            del self.by_id[key]
        except (KeyError, AttributeError):  # pragma: no cover
            pass                            # pragma: no cover
            
    class _keyed_weakref(weakref.ref):
        def __init__(self, object, callback):
            weakref.ref.__init__(self, object, callback)
            self.key = id(object)

    def _ref(self, object):
        return self._keyed_weakref(object, self._cleanup)

import time
if win32 or jython:
    time_func = time.clock
else:
    time_func = time.time 

class LRUCache(dict):
    """Dictionary with 'squishy' removal of least
    recently used items.
    
    """
    def __init__(self, capacity=100, threshold=.5):
        self.capacity = capacity
        self.threshold = threshold

    def __getitem__(self, key):
        item = dict.__getitem__(self, key)
        item[2] = time_func()
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
            item = [key, value, time_func()]
            dict.__setitem__(self, key, item)
        else:
            item[1] = value
        self._manage_size()

    def _manage_size(self):
        while len(self) > self.capacity + self.capacity * self.threshold:
            bytime = sorted(dict.values(self), 
                            key=operator.itemgetter(2),
                            reverse=True)
            for item in bytime[self.capacity:]:
                try:
                    del self[item[0]]
                except KeyError:
                    # if we couldnt find a key, most 
                    # likely some other thread broke in 
                    # on us. loop around and try again
                    break

def warn(msg, stacklevel=3):
    if isinstance(msg, basestring):
        warnings.warn(msg, exc.SAWarning, stacklevel=stacklevel)
    else:
        warnings.warn(msg, stacklevel=stacklevel)

def warn_deprecated(msg, stacklevel=3):
    warnings.warn(msg, exc.SADeprecationWarning, stacklevel=stacklevel)

def warn_pending_deprecation(msg, stacklevel=3):
    warnings.warn(msg, exc.SAPendingDeprecationWarning, stacklevel=stacklevel)

def deprecated(message=None, add_deprecation_to_docstring=True):
    """Decorates a function and issues a deprecation warning on use.

    message
      If provided, issue message in the warning.  A sensible default
      is used if not provided.

    add_deprecation_to_docstring
      Default True.  If False, the wrapped function's __doc__ is left
      as-is.  If True, the 'message' is prepended to the docs if
      provided, or sensible default if message is omitted.
    """

    if add_deprecation_to_docstring:
        header = message is not None and message or 'Deprecated.'
    else:
        header = None

    if message is None:
        message = "Call to deprecated function %(func)s"

    def decorate(fn):
        return _decorate_with_warning(
            fn, exc.SADeprecationWarning,
            message % dict(func=fn.__name__), header)
    return decorate

def pending_deprecation(version, message=None,
                        add_deprecation_to_docstring=True):
    """Decorates a function and issues a pending deprecation warning on use.

    version
      An approximate future version at which point the pending deprecation
      will become deprecated.  Not used in messaging.

    message
      If provided, issue message in the warning.  A sensible default
      is used if not provided.

    add_deprecation_to_docstring
      Default True.  If False, the wrapped function's __doc__ is left
      as-is.  If True, the 'message' is prepended to the docs if
      provided, or sensible default if message is omitted.
    """

    if add_deprecation_to_docstring:
        header = message is not None and message or 'Deprecated.'
    else:
        header = None

    if message is None:
        message = "Call to deprecated function %(func)s"

    def decorate(fn):
        return _decorate_with_warning(
            fn, exc.SAPendingDeprecationWarning,
            message % dict(func=fn.__name__), header)
    return decorate

def _decorate_with_warning(func, wtype, message, docstring_header=None):
    """Wrap a function with a warnings.warn and augmented docstring."""

    @decorator
    def warned(fn, *args, **kwargs):
        warnings.warn(wtype(message), stacklevel=3)
        return fn(*args, **kwargs)

    doc = func.__doc__ is not None and func.__doc__ or ''
    if docstring_header is not None:
        docstring_header %= dict(func=func.__name__)
        docs = doc and doc.expandtabs().split('\n') or []
        indent = ''
        for line in docs[1:]:
            text = line.lstrip()
            if text:
                indent = line[0:len(line) - len(text)]
                break
        point = min(len(docs), 1)
        docs.insert(point, '\n' + indent + docstring_header.rstrip())
        doc = '\n'.join(docs)

    decorated = warned(func)
    decorated.__doc__ = doc
    return decorated

class classproperty(property):
    """A decorator that behaves like @property except that operates
    on classes rather than instances.

    This is helpful when you need to compute __table_args__ and/or
    __mapper_args__ when using declarative."""
    
    def __init__(self, fget, *arg, **kw):
        super(classproperty, self).__init__(fget, *arg, **kw)
        self.__doc__ = fget.__doc__
        
    def __get__(desc, self, cls):
        return desc.fget(cls)

