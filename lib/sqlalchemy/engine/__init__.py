# engine/__init__.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""SQL connections, SQL execution and high-level DB-API interface.

The engine package defines the basic components used to interface
DB-API modules with higher-level statement construction,
connection-management, execution and result contexts.  The primary
"entry point" class into this package is the Engine and its public
constructor ``create_engine()``.

"""

from . import events  # noqa
from . import util  # noqa
from .base import Connection  # noqa
from .base import Engine  # noqa
from .base import NestedTransaction  # noqa
from .base import RootTransaction  # noqa
from .base import Transaction  # noqa
from .base import TwoPhaseTransaction  # noqa
from .create import create_engine
from .create import engine_from_config
from .interfaces import Compiled  # noqa
from .interfaces import Connectable  # noqa
from .interfaces import CreateEnginePlugin  # noqa
from .interfaces import Dialect  # noqa
from .interfaces import ExceptionContext  # noqa
from .interfaces import ExecutionContext  # noqa
from .interfaces import TypeCompiler  # noqa
from .mock import create_mock_engine
from .result import BaseRow  # noqa
from .result import BufferedColumnResultProxy  # noqa
from .result import BufferedColumnRow  # noqa
from .result import BufferedRowResultProxy  # noqa
from .result import FullyBufferedResultProxy  # noqa
from .result import LegacyRow  # noqa
from .result import result_tuple  # noqa
from .result import ResultProxy  # noqa
from .result import Row  # noqa
from .result import RowMapping  # noqa
from .util import connection_memoize  # noqa
from ..sql import ddl  # noqa


__all__ = ("create_engine", "engine_from_config", "create_mock_engine")


def __go(lcls):
    from .. import future
    from . import result

    result._future_Result = future.Result


__go(locals())
