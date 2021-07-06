# engine/util.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

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


class TransactionalContext(object):
    """Apply Python context manager behavior to transaction objects.

    Performs validation to ensure the subject of the transaction is not
    used if the transaction were ended prematurely.

    """

    _trans_subject = None

    def _transaction_is_active(self):
        raise NotImplementedError()

    def _transaction_is_closed(self):
        raise NotImplementedError()

    def _get_subject(self):
        raise NotImplementedError()

    @classmethod
    def _trans_ctx_check(cls, subject):
        trans_context = subject._trans_context_manager
        if trans_context:
            if not trans_context._transaction_is_active():
                raise exc.InvalidRequestError(
                    "Can't operate on closed transaction inside context "
                    "manager.  Please complete the context manager "
                    "before emitting further commands."
                )

    def __enter__(self):
        subject = self._get_subject()

        # none for outer transaction, may be non-None for nested
        # savepoint, legacy nesting cases
        trans_context = subject._trans_context_manager
        self._outer_trans_ctx = trans_context

        self._trans_subject = subject
        subject._trans_context_manager = self
        return self

    def __exit__(self, type_, value, traceback):
        subject = self._trans_subject

        # simplistically we could assume that
        # "subject._trans_context_manager is self".  However, any calling
        # code that is manipulating __exit__ directly would break this
        # assumption.  alembic context manager
        # is an example of partial use that just calls __exit__ and
        # not __enter__ at the moment.  it's safe to assume this is being done
        # in the wild also
        out_of_band_exit = (
            subject is None or subject._trans_context_manager is not self
        )

        if type_ is None and self._transaction_is_active():
            try:
                self.commit()
            except:
                with util.safe_reraise():
                    self.rollback()
            finally:
                if not out_of_band_exit:
                    subject._trans_context_manager = self._outer_trans_ctx
                self._trans_subject = self._outer_trans_ctx = None
        else:
            try:
                if not self._transaction_is_active():
                    if not self._transaction_is_closed():
                        self.close()
                else:
                    self.rollback()
            finally:
                if not out_of_band_exit:
                    subject._trans_context_manager = self._outer_trans_ctx
                self._trans_subject = self._outer_trans_ctx = None
