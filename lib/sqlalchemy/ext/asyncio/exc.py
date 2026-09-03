# ext/asyncio/exc.py
# Copyright (C) 2020-2026 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from ... import exc


class AsyncMethodRequired(exc.InvalidRequestError):
    """an API can't be used because its result would not be
    compatible with async"""


class AsyncContextNotStarted(exc.InvalidRequestError):
    """a startable context manager has not been started."""


class AsyncContextAlreadyStarted(exc.InvalidRequestError):
    """a startable context manager is already started."""


class AsyncBindNotFound(exc.InvalidRequestError):
    """a bind has no asyncio counterpart known to the
    :class:`_asyncio.AsyncSession`.

    .. versionadded:: 2.1

    """
