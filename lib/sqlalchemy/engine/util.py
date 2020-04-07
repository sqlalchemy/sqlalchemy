# engine/util.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .. import exc
from .. import util
from ..util import collections_abc


def connection_memoize(key):
    """Decorator, memoize a function in a connection.info stash.

    Only applicable to functions which take no arguments other than a
    connection.  The memo will be stored in ``connection.info[key]``.
    """

    @util.decorator
    def decorated(fn, self, connection):
        connection = connection.connect()
        try:
            return connection.info[key]
        except KeyError:
            connection.info[key] = val = fn(self, connection)
            return val

    return decorated


def py_fallback():
    # TODO: pass the Connection in so that there can be a standard
    # method for warning on parameter format
    def _distill_params(multiparams, params):  # noqa
        r"""Given arguments from the calling form \*multiparams, \**params,
        return a list of bind parameter structures, usually a list of
        dictionaries.

        In the case of 'raw' execution which accepts positional parameters,
        it may be a list of tuples or lists.

        """

        if not multiparams:
            if params:
                # TODO: parameter format deprecation warning
                return [params]
            else:
                return []
        elif len(multiparams) == 1:
            zero = multiparams[0]
            if isinstance(zero, (list, tuple)):
                if (
                    not zero
                    or hasattr(zero[0], "__iter__")
                    and not hasattr(zero[0], "strip")
                ):
                    # execute(stmt, [{}, {}, {}, ...])
                    # execute(stmt, [(), (), (), ...])
                    return zero
                else:
                    # execute(stmt, ("value", "value"))
                    return [zero]
            elif hasattr(zero, "keys"):
                # execute(stmt, {"key":"value"})
                return [zero]
            else:
                # execute(stmt, "value")
                return [[zero]]
        else:
            # TODO: parameter format deprecation warning
            if hasattr(multiparams[0], "__iter__") and not hasattr(
                multiparams[0], "strip"
            ):
                return multiparams
            else:
                return [multiparams]

    return locals()


_no_tuple = ()
_no_kw = util.immutabledict()


def _distill_params_20(params):
    if params is None:
        return _no_tuple, _no_kw, []
    elif isinstance(params, collections_abc.MutableSequence):  # list
        if params and not isinstance(
            params[0], (collections_abc.Mapping, tuple)
        ):
            raise exc.ArgumentError(
                "List argument must consist only of tuples or dictionaries"
            )

        # the tuple is needed atm by the C version of _distill_params...
        return tuple(params), _no_kw, params
    elif isinstance(
        params,
        (collections_abc.Sequence, collections_abc.Mapping),  # tuple or dict
    ):
        return _no_tuple, params, [params]
    else:
        raise exc.ArgumentError("mapping or sequence expected for parameters")


try:
    from sqlalchemy.cutils import _distill_params  # noqa
except ImportError:
    globals().update(py_fallback())
