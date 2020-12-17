# sqlalchemy/__init__.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from . import util as _util  # noqa
from .inspection import inspect  # noqa
from .schema import BLANK_SCHEMA  # noqa
from .schema import CheckConstraint  # noqa
from .schema import Column  # noqa
from .schema import ColumnDefault  # noqa
from .schema import Computed  # noqa
from .schema import Constraint  # noqa
from .schema import DDL  # noqa
from .schema import DefaultClause  # noqa
from .schema import FetchedValue  # noqa
from .schema import ForeignKey  # noqa
from .schema import ForeignKeyConstraint  # noqa
from .schema import IdentityOptions  # noqa
from .schema import Index  # noqa
from .schema import MetaData  # noqa
from .schema import PassiveDefault  # noqa
from .schema import PrimaryKeyConstraint  # noqa
from .schema import Sequence  # noqa
from .schema import Table  # noqa
from .schema import ThreadLocalMetaData  # noqa
from .schema import UniqueConstraint  # noqa
from .sql import alias  # noqa
from .sql import all_  # noqa
from .sql import and_  # noqa
from .sql import any_  # noqa
from .sql import asc  # noqa
from .sql import between  # noqa
from .sql import bindparam  # noqa
from .sql import case  # noqa
from .sql import cast  # noqa
from .sql import collate  # noqa
from .sql import column  # noqa
from .sql import delete  # noqa
from .sql import desc  # noqa
from .sql import distinct  # noqa
from .sql import except_  # noqa
from .sql import except_all  # noqa
from .sql import exists  # noqa
from .sql import extract  # noqa
from .sql import false  # noqa
from .sql import func  # noqa
from .sql import funcfilter  # noqa
from .sql import insert  # noqa
from .sql import intersect  # noqa
from .sql import intersect_all  # noqa
from .sql import join  # noqa
from .sql import lateral  # noqa
from .sql import literal  # noqa
from .sql import literal_column  # noqa
from .sql import modifier  # noqa
from .sql import not_  # noqa
from .sql import null  # noqa
from .sql import nullsfirst  # noqa
from .sql import nullslast  # noqa
from .sql import or_  # noqa
from .sql import outerjoin  # noqa
from .sql import outparam  # noqa
from .sql import over  # noqa
from .sql import select  # noqa
from .sql import subquery  # noqa
from .sql import table  # noqa
from .sql import tablesample  # noqa
from .sql import text  # noqa
from .sql import true  # noqa
from .sql import tuple_  # noqa
from .sql import type_coerce  # noqa
from .sql import union  # noqa
from .sql import union_all  # noqa
from .sql import update  # noqa
from .sql import within_group  # noqa
from .types import ARRAY  # noqa
from .types import BIGINT  # noqa
from .types import BigInteger  # noqa
from .types import BINARY  # noqa
from .types import Binary  # noqa
from .types import BLOB  # noqa
from .types import BOOLEAN  # noqa
from .types import Boolean  # noqa
from .types import CHAR  # noqa
from .types import CLOB  # noqa
from .types import DATE  # noqa
from .types import Date  # noqa
from .types import DATETIME  # noqa
from .types import DateTime  # noqa
from .types import DECIMAL  # noqa
from .types import Enum  # noqa
from .types import FLOAT  # noqa
from .types import Float  # noqa
from .types import INT  # noqa
from .types import INTEGER  # noqa
from .types import Integer  # noqa
from .types import Interval  # noqa
from .types import JSON  # noqa
from .types import LargeBinary  # noqa
from .types import NCHAR  # noqa
from .types import NUMERIC  # noqa
from .types import Numeric  # noqa
from .types import NVARCHAR  # noqa
from .types import PickleType  # noqa
from .types import REAL  # noqa
from .types import SMALLINT  # noqa
from .types import SmallInteger  # noqa
from .types import String  # noqa
from .types import TEXT  # noqa
from .types import Text  # noqa
from .types import TIME  # noqa
from .types import Time  # noqa
from .types import TIMESTAMP  # noqa
from .types import TypeDecorator  # noqa
from .types import Unicode  # noqa
from .types import UnicodeText  # noqa
from .types import VARBINARY  # noqa
from .types import VARCHAR  # noqa

from .engine import create_engine  # noqa nosort
from .engine import engine_from_config  # noqa nosort


__version__ = "1.3.22"


def __go(lcls):
    global __all__

    from . import events  # noqa
    from . import util as _sa_util

    import inspect as _inspect

    __all__ = sorted(
        name
        for name, obj in lcls.items()
        if not (name.startswith("_") or _inspect.ismodule(obj))
    )

    _sa_util.dependencies.resolve_all("sqlalchemy")

    from . import exc

    exc._version_token = "".join(__version__.split(".")[0:2])


__go(locals())
