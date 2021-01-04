# sqlalchemy/pool/__init__.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Connection pooling for DB-API connections.

Provides a number of connection pool implementations for a variety of
usage scenarios and thread behavior requirements imposed by the
application, DB-API or database itself.

Also provides a DB-API 2.0 connection proxying mechanism allowing
regular DB-API connect() methods to be transparently managed by a
SQLAlchemy connection pool.
"""

from . import events  # noqa
from .base import _ConnectionFairy  # noqa
from .base import _ConnectionRecord  # noqa
from .base import _finalize_fairy  # noqa
from .base import Pool
from .base import reset_commit
from .base import reset_none
from .base import reset_rollback
from .dbapi_proxy import clear_managers
from .dbapi_proxy import manage
from .impl import AssertionPool
from .impl import AsyncAdaptedQueuePool
from .impl import FallbackAsyncAdaptedQueuePool
from .impl import NullPool
from .impl import QueuePool
from .impl import SingletonThreadPool
from .impl import StaticPool


__all__ = [
    "Pool",
    "reset_commit",
    "reset_none",
    "reset_rollback",
    "clear_managers",
    "manage",
    "AssertionPool",
    "NullPool",
    "QueuePool",
    "AsyncAdaptedQueuePool",
    "FallbackAsyncAdaptedQueuePool",
    "SingletonThreadPool",
    "StaticPool",
]

# as these are likely to be used in various test suites, debugging
# setups, keep them in the sqlalchemy.pool namespace
