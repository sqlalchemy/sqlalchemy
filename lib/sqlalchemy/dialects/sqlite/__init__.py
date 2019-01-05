# sqlite/__init__.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.dialects.sqlite.base import BLOB
from sqlalchemy.dialects.sqlite.base import BOOLEAN
from sqlalchemy.dialects.sqlite.base import CHAR
from sqlalchemy.dialects.sqlite.base import DATE
from sqlalchemy.dialects.sqlite.base import DATETIME
from sqlalchemy.dialects.sqlite.base import DECIMAL
from sqlalchemy.dialects.sqlite.base import FLOAT
from sqlalchemy.dialects.sqlite.base import INTEGER
from sqlalchemy.dialects.sqlite.base import NUMERIC
from sqlalchemy.dialects.sqlite.base import REAL
from sqlalchemy.dialects.sqlite.base import SMALLINT
from sqlalchemy.dialects.sqlite.base import TEXT
from sqlalchemy.dialects.sqlite.base import TIME
from sqlalchemy.dialects.sqlite.base import TIMESTAMP
from sqlalchemy.dialects.sqlite.base import VARCHAR
from . import base  # noqa
from . import pysqlcipher  # noqa
from . import pysqlite  # noqa

# default dialect
base.dialect = dialect = pysqlite.dialect


__all__ = (
    "BLOB",
    "BOOLEAN",
    "CHAR",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "FLOAT",
    "INTEGER",
    "NUMERIC",
    "SMALLINT",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "VARCHAR",
    "REAL",
    "dialect",
)
