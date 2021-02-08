# sql/__init__.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .base import Executable
from .compiler import COLLECT_CARTESIAN_PRODUCTS
from .compiler import FROM_LINTING
from .compiler import NO_LINTING
from .compiler import WARN_LINTING
from .expression import Alias
from .expression import alias
from .expression import all_
from .expression import and_
from .expression import any_
from .expression import asc
from .expression import between
from .expression import bindparam
from .expression import case
from .expression import cast
from .expression import ClauseElement
from .expression import collate
from .expression import column
from .expression import ColumnCollection
from .expression import ColumnElement
from .expression import CompoundSelect
from .expression import cte
from .expression import Delete
from .expression import delete
from .expression import desc
from .expression import distinct
from .expression import except_
from .expression import except_all
from .expression import exists
from .expression import extract
from .expression import false
from .expression import False_
from .expression import FromClause
from .expression import func
from .expression import funcfilter
from .expression import Insert
from .expression import insert
from .expression import intersect
from .expression import intersect_all
from .expression import Join
from .expression import join
from .expression import label
from .expression import LABEL_STYLE_DEFAULT
from .expression import LABEL_STYLE_DISAMBIGUATE_ONLY
from .expression import LABEL_STYLE_NONE
from .expression import LABEL_STYLE_TABLENAME_PLUS_COL
from .expression import lambda_stmt
from .expression import LambdaElement
from .expression import lateral
from .expression import literal
from .expression import literal_column
from .expression import modifier
from .expression import not_
from .expression import null
from .expression import nulls_first
from .expression import nulls_last
from .expression import nullsfirst
from .expression import nullslast
from .expression import or_
from .expression import outerjoin
from .expression import outparam
from .expression import over
from .expression import quoted_name
from .expression import Select
from .expression import select
from .expression import Selectable
from .expression import StatementLambdaElement
from .expression import Subquery
from .expression import subquery
from .expression import table
from .expression import TableClause
from .expression import TableSample
from .expression import tablesample
from .expression import text
from .expression import true
from .expression import True_
from .expression import tuple_
from .expression import type_coerce
from .expression import union
from .expression import union_all
from .expression import Update
from .expression import update
from .expression import Values
from .expression import values
from .expression import within_group
from .visitors import ClauseVisitor


def __go(lcls):
    global __all__
    from .. import util as _sa_util

    import inspect as _inspect

    __all__ = sorted(
        name
        for name, obj in lcls.items()
        if not (name.startswith("_") or _inspect.ismodule(obj))
    )

    from .annotation import _prepare_annotations
    from .annotation import Annotated
    from .elements import AnnotatedColumnElement
    from .elements import ClauseList
    from .selectable import AnnotatedFromClause

    # from .traversals import _preconfigure_traversals

    from . import base
    from . import coercions
    from . import elements
    from . import events
    from . import lambdas
    from . import selectable
    from . import schema
    from . import sqltypes
    from . import traversals
    from . import type_api

    base.coercions = elements.coercions = coercions
    base.elements = elements
    base.type_api = type_api
    coercions.elements = elements
    coercions.lambdas = lambdas
    coercions.schema = schema
    coercions.selectable = selectable
    coercions.sqltypes = sqltypes
    coercions.traversals = traversals

    _prepare_annotations(ColumnElement, AnnotatedColumnElement)
    _prepare_annotations(FromClause, AnnotatedFromClause)
    _prepare_annotations(ClauseList, Annotated)

    # this is expensive at import time; elements that are used can create
    # their traversals on demand
    # _preconfigure_traversals(ClauseElement)

    _sa_util.preloaded.import_prefix("sqlalchemy.sql")

    from . import naming


__go(locals())
