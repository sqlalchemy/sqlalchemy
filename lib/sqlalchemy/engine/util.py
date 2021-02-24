# engine/util.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .. import exc
from .. import util
from ..util import collections_abc
from ..util import immutabledict


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


_no_tuple = ()
_no_kw = util.immutabledict()


def _distill_params(connection, multiparams, params):
    r"""Given arguments from the calling form \*multiparams, \**params,
    return a list of bind parameter structures, usually a list of
    dictionaries.

    In the case of 'raw' execution which accepts positional parameters,
    it may be a list of tuples or lists.

    """

    if not multiparams:
        if params:
            connection._warn_for_legacy_exec_format()
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
                # this is used by exec_driver_sql only, so a deprecation
                # warning would already be coming from passing a plain
                # textual statement with positional parameters to
                # execute().
                # execute(stmt, ("value", "value"))
                return [zero]
        elif hasattr(zero, "keys"):
            # execute(stmt, {"key":"value"})
            return [zero]
        else:
            connection._warn_for_legacy_exec_format()
            # execute(stmt, "value")
            return [[zero]]
    else:
        connection._warn_for_legacy_exec_format()
        if hasattr(multiparams[0], "__iter__") and not hasattr(
            multiparams[0], "strip"
        ):
            return multiparams
        else:
            return [multiparams]


def _distill_cursor_params(connection, multiparams, params):
    """_distill_params without any warnings.  more appropriate for
    "cursor" params that can include tuple arguments, lists of tuples,
    etc.

    """

    if not multiparams:
        if params:
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
                # this is used by exec_driver_sql only, so a deprecation
                # warning would already be coming from passing a plain
                # textual statement with positional parameters to
                # execute().
                # execute(stmt, ("value", "value"))

                return [zero]
        elif hasattr(zero, "keys"):
            # execute(stmt, {"key":"value"})
            return [zero]
        else:
            # execute(stmt, "value")
            return [[zero]]
    else:
        if hasattr(multiparams[0], "__iter__") and not hasattr(
            multiparams[0], "strip"
        ):
            return multiparams
        else:
            return [multiparams]


def _distill_params_20(params):
    if params is None:
        return _no_tuple, _no_kw
    elif isinstance(params, list):
        # collections_abc.MutableSequence): # avoid abc.__instancecheck__
        if params and not isinstance(
            params[0], (collections_abc.Mapping, tuple)
        ):
            raise exc.ArgumentError(
                "List argument must consist only of tuples or dictionaries"
            )

        return (params,), _no_kw
    elif isinstance(
        params,
        (tuple, dict, immutabledict),
        # avoid abc.__instancecheck__
        # (collections_abc.Sequence, collections_abc.Mapping),
    ):
        return (params,), _no_kw
    else:
        raise exc.ArgumentError("mapping or sequence expected for parameters")
