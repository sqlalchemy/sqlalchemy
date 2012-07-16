# sqlalchemy/inspect.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Base inspect API.

:func:`.inspect` provides access to a contextual object
regarding a subject.

Various subsections of SQLAlchemy,
such as the :class:`.Inspector`, :class:`.Mapper`, and
others register themselves with the "inspection registry" here
so that they may return a context object given a certain kind
of argument.
"""

from . import util, exc
_registrars = util.defaultdict(list)

def inspect(subject, raiseerr=True):
    type_ = type(subject)
    for cls in type_.__mro__:
        if cls in _registrars:
            reg = _registrars[cls]
            ret = reg(subject)
            if ret is not None:
                break
    else:
        reg = ret = None

    if raiseerr and (
            reg is None or ret is None
        ):
        raise exc.NoInspectionAvailable(
            "No inspection system is "
            "available for object of type %s" %
            type_)
    return ret

def _inspects(*types):
    def decorate(fn_or_cls):
        for type_ in types:
            if type_ in _registrars:
                raise AssertionError(
                            "Type %s is already "
                            "registered" % type_)
            _registrars[type_] = fn_or_cls
        return fn_or_cls
    return decorate

def _self_inspects(*types):
    _inspects(*types)(lambda subject:subject)