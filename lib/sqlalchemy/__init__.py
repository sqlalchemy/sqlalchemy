# sqlalchemy/__init__.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


from .sql import (
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
    false,
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
    over,
    select,
    subquery,
    text,
    true,
    tuple_,
    type_coerce,
    union,
    union_all,
    update,
    )

from .types import (
    BIGINT,
    BINARY,
    BLOB,
    BOOLEAN,
    BigInteger,
    Binary,
    Boolean,
    CHAR,
    CLOB,
    DATE,
    DATETIME,
    DECIMAL,
    Date,
    DateTime,
    Enum,
    FLOAT,
    Float,
    INT,
    INTEGER,
    Integer,
    Interval,
    LargeBinary,
    NCHAR,
    NVARCHAR,
    NUMERIC,
    Numeric,
    PickleType,
    REAL,
    SMALLINT,
    SmallInteger,
    String,
    TEXT,
    TIME,
    TIMESTAMP,
    Text,
    Time,
    TypeDecorator,
    Unicode,
    UnicodeText,
    VARBINARY,
    VARCHAR,
    )


from .schema import (
    CheckConstraint,
    Column,
    ColumnDefault,
    Constraint,
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
    DDL,
)


from .inspection import inspect
from .engine import create_engine, engine_from_config

__version__ = '0.9.9'


def __go(lcls):
    global __all__

    from . import events
    from . import util as _sa_util

    import inspect as _inspect

    __all__ = sorted(name for name, obj in lcls.items()
                     if not (name.startswith('_') or _inspect.ismodule(obj)))

    _sa_util.dependencies.resolve_all("sqlalchemy")
__go(locals())
