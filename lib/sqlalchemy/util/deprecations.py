# util/deprecations.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Helpers related to deprecation of functions, methods, classes, other
functionality."""

import re
import warnings

from . import compat
from .langhelpers import decorator
from .langhelpers import inject_docstring_text
from .langhelpers import inject_param_text
from .. import exc


def warn_deprecated(msg, stacklevel=3):
    warnings.warn(msg, exc.SADeprecationWarning, stacklevel=stacklevel)


def warn_pending_deprecation(msg, stacklevel=3):
    warnings.warn(msg, exc.SAPendingDeprecationWarning, stacklevel=stacklevel)


def deprecated_cls(version, message, constructor="__init__"):
    header = ".. deprecated:: %s %s" % (version, (message or ""))

    def decorate(cls):
        return _decorate_cls_with_warning(
            cls,
            constructor,
            exc.SADeprecationWarning,
            message % dict(func=constructor),
            header,
        )

    return decorate


def deprecated(version, message=None, add_deprecation_to_docstring=True):
    """Decorates a function and issues a deprecation warning on use.

    :param version:
      Issue version in the warning.

    :param message:
      If provided, issue message in the warning.  A sensible default
      is used if not provided.

    :param add_deprecation_to_docstring:
      Default True.  If False, the wrapped function's __doc__ is left
      as-is.  If True, the 'message' is prepended to the docs if
      provided, or sensible default if message is omitted.

    """

    if add_deprecation_to_docstring:
        header = ".. deprecated:: %s %s" % (version, (message or ""))
    else:
        header = None

    if message is None:
        message = "Call to deprecated function %(func)s"

    def decorate(fn):
        return _decorate_with_warning(
            fn,
            exc.SADeprecationWarning,
            message % dict(func=fn.__name__),
            header,
        )

    return decorate


def deprecated_params(**specs):
    """Decorates a function to warn on use of certain parameters.

    e.g. ::

        @deprecated_params(
            weak_identity_map=(
                "0.7",
                "the :paramref:`.Session.weak_identity_map parameter "
                "is deprecated."
            )

        )

    """

    messages = {}
    for param, (version, message) in specs.items():
        messages[param] = _sanitize_restructured_text(message)

    def decorate(fn):
        spec = compat.inspect_getfullargspec(fn)
        if spec.defaults is not None:
            defaults = dict(
                zip(
                    spec.args[(len(spec.args) - len(spec.defaults)) :],
                    spec.defaults,
                )
            )
            check_defaults = set(defaults).intersection(messages)
            check_kw = set(messages).difference(defaults)
        else:
            check_defaults = ()
            check_kw = set(messages)

        @decorator
        def warned(fn, *args, **kwargs):
            for m in check_defaults:
                if kwargs[m] != defaults[m]:
                    warnings.warn(
                        messages[m], exc.SADeprecationWarning, stacklevel=3
                    )
            for m in check_kw:
                if m in kwargs:
                    warnings.warn(
                        messages[m], exc.SADeprecationWarning, stacklevel=3
                    )

            return fn(*args, **kwargs)

        doc = fn.__doc__ is not None and fn.__doc__ or ""
        if doc:
            doc = inject_param_text(
                doc,
                {
                    param: ".. deprecated:: %s %s" % (version, (message or ""))
                    for param, (version, message) in specs.items()
                },
            )
        decorated = warned(fn)
        decorated.__doc__ = doc
        return decorated

    return decorate


def pending_deprecation(
    version, message=None, add_deprecation_to_docstring=True
):
    """Decorates a function and issues a pending deprecation warning on use.

    :param version:
      An approximate future version at which point the pending deprecation
      will become deprecated.  Not used in messaging.

    :param message:
      If provided, issue message in the warning.  A sensible default
      is used if not provided.

    :param add_deprecation_to_docstring:
      Default True.  If False, the wrapped function's __doc__ is left
      as-is.  If True, the 'message' is prepended to the docs if
      provided, or sensible default if message is omitted.
    """

    if add_deprecation_to_docstring:
        header = ".. deprecated:: %s (pending) %s" % (version, (message or ""))
    else:
        header = None

    if message is None:
        message = "Call to deprecated function %(func)s"

    def decorate(fn):
        return _decorate_with_warning(
            fn,
            exc.SAPendingDeprecationWarning,
            message % dict(func=fn.__name__),
            header,
        )

    return decorate


def deprecated_option_value(parameter_value, default_value, warning_text):
    if parameter_value is None:
        return default_value
    else:
        warn_deprecated(warning_text)
        return parameter_value


def _sanitize_restructured_text(text):
    def repl(m):
        type_, name = m.group(1, 2)
        if type_ in ("func", "meth"):
            name += "()"
        return name

    return re.sub(r"\:(\w+)\:`~?\.?(.+?)`", repl, text)


def _decorate_cls_with_warning(
    cls, constructor, wtype, message, docstring_header=None
):
    doc = cls.__doc__ is not None and cls.__doc__ or ""
    if docstring_header is not None:
        docstring_header %= dict(func=constructor)

        doc = inject_docstring_text(doc, docstring_header, 1)

        if type(cls) is type:
            clsdict = dict(cls.__dict__)
            clsdict["__doc__"] = doc
            cls = type(cls.__name__, cls.__bases__, clsdict)
            constructor_fn = clsdict[constructor]
        else:
            cls.__doc__ = doc
            constructor_fn = getattr(cls, constructor)

    setattr(
        cls,
        constructor,
        _decorate_with_warning(constructor_fn, wtype, message, None),
    )

    return cls


def _decorate_with_warning(func, wtype, message, docstring_header=None):
    """Wrap a function with a warnings.warn and augmented docstring."""

    message = _sanitize_restructured_text(message)

    @decorator
    def warned(fn, *args, **kwargs):
        warnings.warn(message, wtype, stacklevel=3)
        return fn(*args, **kwargs)

    doc = func.__doc__ is not None and func.__doc__ or ""
    if docstring_header is not None:
        docstring_header %= dict(func=func.__name__)

        doc = inject_docstring_text(doc, docstring_header, 1)

    decorated = warned(func)
    decorated.__doc__ = doc
    decorated._sa_warn = lambda: warnings.warn(message, wtype, stacklevel=3)
    return decorated
