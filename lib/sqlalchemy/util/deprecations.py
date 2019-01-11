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

from .langhelpers import decorator
from .. import exc


def warn_deprecated(msg, stacklevel=3):
    warnings.warn(msg, exc.SADeprecationWarning, stacklevel=stacklevel)


def warn_pending_deprecation(msg, stacklevel=3):
    warnings.warn(msg, exc.SAPendingDeprecationWarning, stacklevel=stacklevel)


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


def _sanitize_restructured_text(text):
    def repl(m):
        type_, name = m.group(1, 2)
        if type_ in ("func", "meth"):
            name += "()"
        return name

    return re.sub(r"\:(\w+)\:`~?\.?(.+?)`", repl, text)


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
