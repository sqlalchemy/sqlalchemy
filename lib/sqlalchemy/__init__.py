# __init__.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import inspect
import sys

import sqlalchemy.exc as exceptions
sys.modules['sqlalchemy.exceptions'] = exceptions

from sqlalchemy.types import (
    BLOB,
    BOOLEAN,
    Binary,
    Boolean,
    CHAR,
    CLOB,
    DATE,
    DATETIME,
    DECIMAL,
    Date,
    DateTime,
    FLOAT,
    Float,
    INT,
    Integer,
    Interval,
    NCHAR,
    NUMERIC,
    Numeric,
    PickleType,
    SMALLINT,
    SmallInteger,
    String,
    TEXT,
    TIME,
    TIMESTAMP,
    Text,
    Time,
    Unicode,
    UnicodeText,
    VARCHAR,
    )

from sqlalchemy.sql import (
    alias,
    and_,
    asc,
    between,
    bindparam,
    case,
    cast,
    collate,
    delete,
    desc,
    distinct,
    except_,
    except_all,
    exists,
    extract,
    func,
    insert,
    intersect,
    intersect_all,
    join,
    literal,
    literal_column,
    modifier,
    not_,
    null,
    or_,
    outerjoin,
    outparam,
    select,
    subquery,
    text,
    union,
    union_all,
    update,
    )

from sqlalchemy.schema import (
    CheckConstraint,
    Column,
    ColumnDefault,
    Constraint,
    DDL,
    DefaultClause,
    FetchedValue,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    MetaData,
    PassiveDefault,
    PrimaryKeyConstraint,
    Sequence,
    Table,
    ThreadLocalMetaData,
    UniqueConstraint,
    )

from sqlalchemy.engine import create_engine, engine_from_config


__all__ = sorted(name for name, obj in locals().items()
                 if not (name.startswith('_') or inspect.ismodule(obj)))
                 
__version__ = '0.5.6'

del inspect, sys
