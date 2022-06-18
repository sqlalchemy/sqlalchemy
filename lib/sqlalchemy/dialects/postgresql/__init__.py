# postgresql/__init__.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

from types import ModuleType

from . import asyncpg  # noqa
from . import base
from . import pg8000  # noqa
from . import psycopg  # noqa
from . import psycopg2  # noqa
from . import psycopg2cffi  # noqa
from .array import All
from .array import Any
from .array import ARRAY
from .array import array
from .base import BIGINT
from .base import BOOLEAN
from .base import CHAR
from .base import DATE
from .base import DOUBLE_PRECISION
from .base import FLOAT
from .base import INTEGER
from .base import NUMERIC
from .base import REAL
from .base import SMALLINT
from .base import TEXT
from .base import UUID
from .base import VARCHAR
from .dml import Insert
from .dml import insert
from .ext import aggregate_order_by
from .ext import array_agg
from .ext import ExcludeConstraint
from .hstore import HSTORE
from .hstore import hstore
from .json import JSON
from .json import JSONB
from .ranges import DATERANGE
from .ranges import INT4RANGE
from .ranges import INT8RANGE
from .ranges import NUMRANGE
from .ranges import TSRANGE
from .ranges import TSTZRANGE
from .types import BIT
from .types import BYTEA
from .types import CIDR
from .types import CreateEnumType
from .types import DropEnumType
from .types import ENUM
from .types import INET
from .types import INTERVAL
from .types import MACADDR
from .types import MONEY
from .types import OID
from .types import REGCLASS
from .types import TIME
from .types import TIMESTAMP
from .types import TSVECTOR

# Alias psycopg also as psycopg_async
psycopg_async = type(
    "psycopg_async", (ModuleType,), {"dialect": psycopg.dialect_async}
)

base.dialect = dialect = psycopg2.dialect


__all__ = (
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "VARCHAR",
    "CHAR",
    "TEXT",
    "NUMERIC",
    "FLOAT",
    "REAL",
    "INET",
    "CIDR",
    "UUID",
    "BIT",
    "MACADDR",
    "MONEY",
    "OID",
    "REGCLASS",
    "DOUBLE_PRECISION",
    "TIMESTAMP",
    "TIME",
    "DATE",
    "BYTEA",
    "BOOLEAN",
    "INTERVAL",
    "ARRAY",
    "ENUM",
    "dialect",
    "array",
    "HSTORE",
    "hstore",
    "INT4RANGE",
    "INT8RANGE",
    "NUMRANGE",
    "DATERANGE",
    "TSVECTOR",
    "TSRANGE",
    "TSTZRANGE",
    "JSON",
    "JSONB",
    "Any",
    "All",
    "DropEnumType",
    "CreateEnumType",
    "ExcludeConstraint",
    "aggregate_order_by",
    "array_agg",
    "insert",
    "Insert",
)
