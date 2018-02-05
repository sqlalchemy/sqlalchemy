# sqlite/__init__.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from . import base, pysqlite, pysqlcipher  # noqa

from sqlalchemy.dialects.sqlite.base import (
    BLOB, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, FLOAT, INTEGER, REAL,
    NUMERIC, SMALLINT, TEXT, TIME, TIMESTAMP, VARCHAR
)

# default dialect
base.dialect = dialect = pysqlite.dialect


__all__ = ('BLOB', 'BOOLEAN', 'CHAR', 'DATE', 'DATETIME', 'DECIMAL',
           'FLOAT', 'INTEGER', 'NUMERIC', 'SMALLINT', 'TEXT', 'TIME',
           'TIMESTAMP', 'VARCHAR', 'REAL', 'dialect')
