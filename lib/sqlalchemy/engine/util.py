# engine/util.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from .. import exc
from .. import util

try:
    from sqlalchemy.cyextension.util import _distill_params_20  # noqa
    from sqlalchemy.cyextension.util import _distill_raw_params  # noqa
except ImportError:
    from ._py_util import _distill_params_20  # noqa
    from ._py_util import _distill_raw_params  # noqa


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


class TransactionalContext:
    """Apply Python context manager behavior to transaction objects.

    Performs validation to ensure the subject of the transaction is not
    used if the transaction were ended prematurely.

    """

    __slots__ = ("_outer_trans_ctx", "_trans_subject")

    def _transaction_is_active(self):
        raise NotImplementedError()

    def _transaction_is_closed(self):
        raise NotImplementedError()

    def _rollback_can_be_called(self):
        """indicates the object is in a state that is known to be acceptable
        for rollback() to be called.

        This does not necessarily mean rollback() will succeed or not raise
        an error, just that there is currently no state detected that indicates
        rollback() would fail or emit warnings.

        It also does not mean that there's a transaction in progress, as
        it is usually safe to call rollback() even if no transaction is
        present.

        .. versionadded:: 1.4.28

        """
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
                    if self._rollback_can_be_called():
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
                    if self._rollback_can_be_called():
                        self.rollback()
            finally:
                if not out_of_band_exit:
                    subject._trans_context_manager = self._outer_trans_ctx
                self._trans_subject = self._outer_trans_ctx = None
