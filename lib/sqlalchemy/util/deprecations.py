# util/deprecations.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Helpers related to deprecation of functions, methods, classes, other
functionality."""

import re
import textwrap
import warnings

from . import compat
from .langhelpers import decorator
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

        has_kw = spec.varkw is not None

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


def _dedent_docstring(text):
    split_text = text.split("\n", 1)
    if len(split_text) == 1:
        return text
    else:
        firstline, remaining = split_text
    if not firstline.startswith(" "):
        return firstline + "\n" + textwrap.dedent(remaining)
    else:
        return textwrap.dedent(text)


def inject_docstring_text(doctext, injecttext, pos):
    doctext = _dedent_docstring(doctext or "")
    lines = doctext.split("\n")
    injectlines = textwrap.dedent(injecttext).split("\n")
    if injectlines[0]:
        injectlines.insert(0, "")

    blanks = [num for num, line in enumerate(lines) if not line.strip()]
    blanks.insert(0, 0)

    inject_pos = blanks[min(pos, len(blanks) - 1)]

    lines = lines[0:inject_pos] + injectlines + lines[inject_pos:]
    return "\n".join(lines)


def inject_param_text(doctext, inject_params):
    doclines = doctext.splitlines()
    lines = []

    to_inject = None
    while doclines:
        line = doclines.pop(0)
        if to_inject is None:
            m = re.match(r"(\s+):param (.+?):", line)
            if m:
                param = m.group(2)
                if param in inject_params:
                    # default indent to that of :param: plus one
                    indent = " " * len(m.group(1)) + " "

                    # but if the next line has text, use that line's
                    # indentntation
                    if doclines:
                        m2 = re.match(r"(\s+)\S", doclines[0])
                        if m2:
                            indent = " " * len(m2.group(1))

                    to_inject = indent + inject_params[param]
        elif not line.rstrip():
            lines.append(line)
            lines.append(to_inject)
            lines.append("\n")
            to_inject = None
        lines.append(line)

    return "\n".join(lines)
