# sqlalchemy/__init__.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from . import util as _util
from .engine import create_engine as create_engine
from .engine import create_mock_engine as create_mock_engine
from .engine import engine_from_config as engine_from_config
from .inspection import inspect as inspect
from .schema import BLANK_SCHEMA as BLANK_SCHEMA
from .schema import CheckConstraint as CheckConstraint
from .schema import Column as Column
from .schema import ColumnDefault as ColumnDefault
from .schema import Computed as Computed
from .schema import Constraint as Constraint
from .schema import DDL as DDL
from .schema import DefaultClause as DefaultClause
from .schema import FetchedValue as FetchedValue
from .schema import ForeignKey as ForeignKey
from .schema import ForeignKeyConstraint as ForeignKeyConstraint
from .schema import Identity as Identity
from .schema import Index as Index
from .schema import MetaData as MetaData
from .schema import PrimaryKeyConstraint as PrimaryKeyConstraint
from .schema import Sequence as Sequence
from .schema import Table as Table
from .schema import UniqueConstraint as UniqueConstraint
from .sql import alias as alias
from .sql import all_ as all_
from .sql import and_ as and_
from .sql import any_ as any_
from .sql import asc as asc
from .sql import between as between
from .sql import bindparam as bindparam
from .sql import case as case
from .sql import cast as cast
from .sql import collate as collate
from .sql import column as column
from .sql import delete as delete
from .sql import desc as desc
from .sql import distinct as distinct
from .sql import except_ as except_
from .sql import except_all as except_all
from .sql import exists as exists
from .sql import extract as extract
from .sql import false as false
from .sql import func as func
from .sql import funcfilter as funcfilter
from .sql import insert as insert
from .sql import intersect as intersect
from .sql import intersect_all as intersect_all
from .sql import join as join
from .sql import label as label
from .sql import LABEL_STYLE_DEFAULT as LABEL_STYLE_DEFAULT
from .sql import (
    LABEL_STYLE_DISAMBIGUATE_ONLY as LABEL_STYLE_DISAMBIGUATE_ONLY,
)
from .sql import LABEL_STYLE_NONE as LABEL_STYLE_NONE
from .sql import (
    LABEL_STYLE_TABLENAME_PLUS_COL as LABEL_STYLE_TABLENAME_PLUS_COL,
)
from .sql import lambda_stmt as lambda_stmt
from .sql import lateral as lateral
from .sql import literal as literal
from .sql import literal_column as literal_column
from .sql import modifier as modifier
from .sql import not_ as not_
from .sql import null as null
from .sql import nulls_first as nulls_first
from .sql import nulls_last as nulls_last
from .sql import nullsfirst as nullsfirst
from .sql import nullslast as nullslast
from .sql import or_ as or_
from .sql import outerjoin as outerjoin
from .sql import outparam as outparam
from .sql import over as over
from .sql import select as select
from .sql import table as table
from .sql import tablesample as tablesample
from .sql import text as text
from .sql import true as true
from .sql import tuple_ as tuple_
from .sql import type_coerce as type_coerce
from .sql import union as union
from .sql import union_all as union_all
from .sql import update as update
from .sql import values as values
from .sql import within_group as within_group
from .types import ARRAY as ARRAY
from .types import BIGINT as BIGINT
from .types import BigInteger as BigInteger
from .types import BINARY as BINARY
from .types import BLOB as BLOB
from .types import BOOLEAN as BOOLEAN
from .types import Boolean as Boolean
from .types import CHAR as CHAR
from .types import CLOB as CLOB
from .types import DATE as DATE
from .types import Date as Date
from .types import DATETIME as DATETIME
from .types import DateTime as DateTime
from .types import DECIMAL as DECIMAL
from .types import Enum as Enum
from .types import FLOAT as FLOAT
from .types import Float as Float
from .types import INT as INT
from .types import INTEGER as INTEGER
from .types import Integer as Integer
from .types import Interval as Interval
from .types import JSON as JSON
from .types import LargeBinary as LargeBinary
from .types import NCHAR as NCHAR
from .types import NUMERIC as NUMERIC
from .types import Numeric as Numeric
from .types import NVARCHAR as NVARCHAR
from .types import PickleType as PickleType
from .types import REAL as REAL
from .types import SMALLINT as SMALLINT
from .types import SmallInteger as SmallInteger
from .types import String as String
from .types import TEXT as TEXT
from .types import Text as Text
from .types import TIME as TIME
from .types import UUID as UUID
from .types import Time as Time
from .types import TIMESTAMP as TIMESTAMP
from .types import TupleType as TupleType
from .types import TypeDecorator as TypeDecorator
from .types import Unicode as Unicode
from .types import UnicodeText as UnicodeText
from .types import VARBINARY as VARBINARY
from .types import VARCHAR as VARCHAR


__version__ = "2.0.0b1"


def __go(lcls):
    from . import util as _sa_util

    _sa_util.preloaded.import_prefix("sqlalchemy")

    from . import exc

    exc._version_token = "".join(__version__.split(".")[0:2])


__go(locals())
