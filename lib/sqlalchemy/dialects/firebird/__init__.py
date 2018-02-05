# firebird/__init__.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from . import base, kinterbasdb, fdb  # noqa

from sqlalchemy.dialects.firebird.base import \
    SMALLINT, BIGINT, FLOAT, DATE, TIME, \
    TEXT, NUMERIC, TIMESTAMP, VARCHAR, CHAR, BLOB

base.dialect = dialect = fdb.dialect

__all__ = (
    'SMALLINT', 'BIGINT', 'FLOAT', 'FLOAT', 'DATE', 'TIME',
    'TEXT', 'NUMERIC', 'FLOAT', 'TIMESTAMP', 'VARCHAR', 'CHAR', 'BLOB',
    'dialect'
)
