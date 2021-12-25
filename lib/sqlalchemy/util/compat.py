# util/compat.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Handle Python version/platform incompatibilities."""
import base64
import collections
import dataclasses
import inspect
import operator
import platform
import sys

py311 = sys.version_info >= (3, 11)
py39 = sys.version_info >= (3, 9)
py38 = sys.version_info >= (3, 8)
pypy = platform.python_implementation() == "PyPy"
cpython = platform.python_implementation() == "CPython"

win32 = sys.platform.startswith("win")
osx = sys.platform.startswith("darwin")
arm = "aarch" in platform.machine().lower()

has_refcount_gc = bool(cpython)

dottedgetter = operator.attrgetter
namedtuple = collections.namedtuple
next = next  # noqa

FullArgSpec = collections.namedtuple(
    "FullArgSpec",
    [
        "args",
        "varargs",
        "varkw",
        "defaults",
        "kwonlyargs",
        "kwonlydefaults",
        "annotations",
    ],
)


try:
    import threading
except ImportError:
    import dummy_threading as threading  # noqa


def inspect_getfullargspec(func):
    """Fully vendored version of getfullargspec from Python 3.3."""

    if inspect.ismethod(func):
        func = func.__func__
    if not inspect.isfunction(func):
        raise TypeError("{!r} is not a Python function".format(func))

    co = func.__code__
    if not inspect.iscode(co):
        raise TypeError("{!r} is not a code object".format(co))

    nargs = co.co_argcount
    names = co.co_varnames
    nkwargs = co.co_kwonlyargcount
    args = list(names[:nargs])
    kwonlyargs = list(names[nargs : nargs + nkwargs])

    nargs += nkwargs
    varargs = None
    if co.co_flags & inspect.CO_VARARGS:
        varargs = co.co_varnames[nargs]
        nargs = nargs + 1
    varkw = None
    if co.co_flags & inspect.CO_VARKEYWORDS:
        varkw = co.co_varnames[nargs]

    return FullArgSpec(
        args,
        varargs,
        varkw,
        func.__defaults__,
        kwonlyargs,
        func.__kwdefaults__,
        func.__annotations__,
    )


if py38:
    from importlib import metadata as importlib_metadata
else:
    import importlib_metadata  # noqa


if py39:
    # pep 584 dict union
    dict_union = operator.or_  # noqa
else:

    def dict_union(a: dict, b: dict) -> dict:
        a = a.copy()
        a.update(b)
        return a


def importlib_metadata_get(group):
    ep = importlib_metadata.entry_points()
    if hasattr(ep, "select"):
        return ep.select(group=group)
    else:
        return ep.get(group, ())


def b(s):
    return s.encode("latin-1")


def b64decode(x):
    return base64.b64decode(x.encode("ascii"))


def b64encode(x):
    return base64.b64encode(x).decode("ascii")


def decode_backslashreplace(text, encoding):
    return text.decode(encoding, errors="backslashreplace")


def cmp(a, b):
    return (a > b) - (a < b)


def raise_(exception, with_traceback=None, replace_context=None, from_=False):
    r"""implement "raise" with cause support.

    :param exception: exception to raise
    :param with_traceback: will call exception.with_traceback()
    :param replace_context: an as-yet-unsupported feature.  This is
        an exception object which we are "replacing", e.g., it's our
        "cause" but we don't want it printed.    Basically just what
        ``__suppress_context__`` does but we don't want to suppress
        the enclosing context, if any.  So for now we make it the
        cause.
    :param from\_: the cause.  this actually sets the cause and doesn't
        hope to hide it someday.

    """
    if with_traceback is not None:
        exception = exception.with_traceback(with_traceback)

    if from_ is not False:
        exception.__cause__ = from_
    elif replace_context is not None:
        # no good solution here, we would like to have the exception
        # have only the context of replace_context.__context__ so that the
        # intermediary exception does not change, but we can't figure
        # that out.
        exception.__cause__ = replace_context

    try:
        raise exception
    finally:
        # credit to
        # https://cosmicpercolator.com/2016/01/13/exception-leaks-in-python-2-and-3/
        # as the __traceback__ object creates a cycle
        del exception, replace_context, from_, with_traceback


def _formatannotation(annotation, base_module=None):
    """vendored from python 3.7"""

    if getattr(annotation, "__module__", None) == "typing":
        return repr(annotation).replace("typing.", "")
    if isinstance(annotation, type):
        if annotation.__module__ in ("builtins", base_module):
            return annotation.__qualname__
        return annotation.__module__ + "." + annotation.__qualname__
    return repr(annotation)


def inspect_formatargspec(
    args,
    varargs=None,
    varkw=None,
    defaults=None,
    kwonlyargs=(),
    kwonlydefaults={},
    annotations={},
    formatarg=str,
    formatvarargs=lambda name: "*" + name,
    formatvarkw=lambda name: "**" + name,
    formatvalue=lambda value: "=" + repr(value),
    formatreturns=lambda text: " -> " + text,
    formatannotation=_formatannotation,
):
    """Copy formatargspec from python 3.7 standard library.

    Python 3 has deprecated formatargspec and requested that Signature
    be used instead, however this requires a full reimplementation
    of formatargspec() in terms of creating Parameter objects and such.
    Instead of introducing all the object-creation overhead and having
    to reinvent from scratch, just copy their compatibility routine.

    Ultimately we would need to rewrite our "decorator" routine completely
    which is not really worth it right now, until all Python 2.x support
    is dropped.

    """

    kwonlydefaults = kwonlydefaults or {}
    annotations = annotations or {}

    def formatargandannotation(arg):
        result = formatarg(arg)
        if arg in annotations:
            result += ": " + formatannotation(annotations[arg])
        return result

    specs = []
    if defaults:
        firstdefault = len(args) - len(defaults)
    for i, arg in enumerate(args):
        spec = formatargandannotation(arg)
        if defaults and i >= firstdefault:
            spec = spec + formatvalue(defaults[i - firstdefault])
        specs.append(spec)

    if varargs is not None:
        specs.append(formatvarargs(formatargandannotation(varargs)))
    else:
        if kwonlyargs:
            specs.append("*")

    if kwonlyargs:
        for kwonlyarg in kwonlyargs:
            spec = formatargandannotation(kwonlyarg)
            if kwonlydefaults and kwonlyarg in kwonlydefaults:
                spec += formatvalue(kwonlydefaults[kwonlyarg])
            specs.append(spec)

    if varkw is not None:
        specs.append(formatvarkw(formatargandannotation(varkw)))

    result = "(" + ", ".join(specs) + ")"
    if "return" in annotations:
        result += formatreturns(formatannotation(annotations["return"]))
    return result


def dataclass_fields(cls):
    """Return a sequence of all dataclasses.Field objects associated
    with a class."""

    if dataclasses.is_dataclass(cls):
        return dataclasses.fields(cls)
    else:
        return []


def local_dataclass_fields(cls):
    """Return a sequence of all dataclasses.Field objects associated with
    a class, excluding those that originate from a superclass."""

    if dataclasses.is_dataclass(cls):
        super_fields = set()
        for sup in cls.__bases__:
            super_fields.update(dataclass_fields(sup))
        return [f for f in dataclasses.fields(cls) if f not in super_fields]
    else:
        return []


def raise_from_cause(exception, exc_info=None):
    r"""legacy.  use raise\_()"""

    if exc_info is None:
        exc_info = sys.exc_info()
    exc_type, exc_value, exc_tb = exc_info
    cause = exc_value if exc_value is not exception else None
    reraise(type(exception), exception, tb=exc_tb, cause=cause)


def reraise(tp, value, tb=None, cause=None):
    r"""legacy.  use raise\_()"""

    raise_(value, with_traceback=tb, from_=cause)
