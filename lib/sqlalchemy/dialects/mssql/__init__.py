# mssql/__init__.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from . import base, pyodbc, adodbapi, pymssql, zxjdbc, mxodbc  # noqa

from .base import \
    INTEGER, BIGINT, SMALLINT, TINYINT, VARCHAR, NVARCHAR, CHAR, \
    NCHAR, TEXT, NTEXT, DECIMAL, NUMERIC, FLOAT, DATETIME,\
    DATETIME2, DATETIMEOFFSET, DATE, TIME, SMALLDATETIME, \
    BINARY, VARBINARY, BIT, REAL, IMAGE, TIMESTAMP, ROWVERSION, \
    MONEY, SMALLMONEY, UNIQUEIDENTIFIER, SQL_VARIANT, XML

base.dialect = dialect = pyodbc.dialect


__all__ = (
    'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'VARCHAR', 'NVARCHAR', 'CHAR',
    'NCHAR', 'TEXT', 'NTEXT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'DATETIME',
    'DATETIME2', 'DATETIMEOFFSET', 'DATE', 'TIME', 'SMALLDATETIME',
    'BINARY', 'VARBINARY', 'BIT', 'REAL', 'IMAGE', 'TIMESTAMP', 'ROWVERSION',
    'MONEY', 'SMALLMONEY', 'UNIQUEIDENTIFIER', 'SQL_VARIANT', 'XML', 'dialect'
)
