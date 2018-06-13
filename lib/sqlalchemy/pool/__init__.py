# sqlalchemy/pool/__init__.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
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

from .base import _refs  # noqa
from .base import Pool  # noqa
from .impl import (  # noqa
    QueuePool, StaticPool, NullPool, AssertionPool, SingletonThreadPool)
from .dbapi_proxy import manage, clear_managers  # noqa

from .base import reset_rollback, reset_commit, reset_none  # noqa

# as these are likely to be used in various test suites, debugging
# setups, keep them in the sqlalchemy.pool namespace
from .base import _ConnectionFairy, _ConnectionRecord, _finalize_fairy  # noqa
