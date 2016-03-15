# types.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Compatibility namespace for sqlalchemy.sql.types.

"""

__all__ = ['TypeEngine', 'TypeDecorator', 'UserDefinedType',
           'INT', 'CHAR', 'VARCHAR', 'NCHAR', 'NVARCHAR', 'TEXT', 'Text',
           'FLOAT', 'NUMERIC', 'REAL', 'DECIMAL', 'TIMESTAMP', 'DATETIME',
           'CLOB', 'BLOB', 'BINARY', 'VARBINARY', 'BOOLEAN', 'BIGINT',
           'SMALLINT', 'INTEGER', 'DATE', 'TIME', 'String', 'Integer',
           'SmallInteger', 'BigInteger', 'Numeric', 'Float', 'DateTime',
           'Date', 'Time', 'LargeBinary', 'Binary', 'Boolean', 'Unicode',
           'Concatenable', 'UnicodeText', 'PickleType', 'Interval', 'Enum',
           'Indexable', 'ARRAY', 'JSON']

from .sql.type_api import (
    adapt_type,
    TypeEngine,
    TypeDecorator,
    Variant,
    to_instance,
    UserDefinedType
)
from .sql.sqltypes import (
    ARRAY,
    BIGINT,
    BINARY,
    BLOB,
    BOOLEAN,
    BigInteger,
    Binary,
    _Binary,
    Boolean,
    CHAR,
    CLOB,
    Concatenable,
    DATE,
    DATETIME,
    DECIMAL,
    Date,
    DateTime,
    Enum,
    FLOAT,
    Float,
    Indexable,
    INT,
    INTEGER,
    Integer,
    Interval,
    JSON,
    LargeBinary,
    MatchType,
    NCHAR,
    NVARCHAR,
    NullType,
    NULLTYPE,
    NUMERIC,
    Numeric,
    PickleType,
    REAL,
    SchemaType,
    SMALLINT,
    SmallInteger,
    String,
    STRINGTYPE,
    TEXT,
    TIME,
    TIMESTAMP,
    Text,
    Time,
    Unicode,
    UnicodeText,
    VARBINARY,
    VARCHAR,
    )
