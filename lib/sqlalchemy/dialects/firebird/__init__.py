# dialects/firebird/__init__.py
# Copyright (C) 2005-2024 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from sqlalchemy.dialects.firebird.base import BIGINT
from sqlalchemy.dialects.firebird.base import BLOB
from sqlalchemy.dialects.firebird.base import CHAR
from sqlalchemy.dialects.firebird.base import DATE
from sqlalchemy.dialects.firebird.base import FLOAT
from sqlalchemy.dialects.firebird.base import NUMERIC
from sqlalchemy.dialects.firebird.base import SMALLINT
from sqlalchemy.dialects.firebird.base import TEXT
from sqlalchemy.dialects.firebird.base import TIME
from sqlalchemy.dialects.firebird.base import TIMESTAMP
from sqlalchemy.dialects.firebird.base import VARCHAR
from . import base  # noqa
from . import fdb  # noqa
from . import kinterbasdb  # noqa


base.dialect = dialect = fdb.dialect

__all__ = (
    "SMALLINT",
    "BIGINT",
    "FLOAT",
    "FLOAT",
    "DATE",
    "TIME",
    "TEXT",
    "NUMERIC",
    "FLOAT",
    "TIMESTAMP",
    "VARCHAR",
    "CHAR",
    "BLOB",
    "dialect",
)
