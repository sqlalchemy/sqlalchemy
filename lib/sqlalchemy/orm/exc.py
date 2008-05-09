# exc.py - ORM exceptions
# Copyright (C) the SQLAlchemy authors and contributors
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""SQLAlchemy ORM exceptions."""

import sqlalchemy.exceptions as sa_exc


class ConcurrentModificationError(sa_exc.SQLAlchemyError):
    """Rows have been modified outside of the unit of work."""


class FlushError(sa_exc.SQLAlchemyError):
    """A invalid condition was detected during flush()."""


class ObjectDeletedError(sa_exc.InvalidRequestError):
    """An refresh() operation failed to re-retrieve an object's row."""


class UnmappedColumnError(sa_exc.InvalidRequestError):
    """Mapping operation was requested on an unknown column."""


# Legacy compat until 0.6.
sa_exc.ConcurrentModificationError = ConcurrentModificationError
sa_exc.FlushError = FlushError
sa_exc.UnmappedColumnError
