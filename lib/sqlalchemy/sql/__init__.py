# sql/__init__.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .expression import Alias  # noqa
from .expression import alias  # noqa
from .expression import all_  # noqa
from .expression import and_  # noqa
from .expression import any_  # noqa
from .expression import asc  # noqa
from .expression import between  # noqa
from .expression import bindparam  # noqa
from .expression import case  # noqa
from .expression import cast  # noqa
from .expression import ClauseElement  # noqa
from .expression import collate  # noqa
from .expression import column  # noqa
from .expression import ColumnCollection  # noqa
from .expression import ColumnElement  # noqa
from .expression import CompoundSelect  # noqa
from .expression import cte  # noqa
from .expression import Delete  # noqa
from .expression import delete  # noqa
from .expression import desc  # noqa
from .expression import distinct  # noqa
from .expression import except_  # noqa
from .expression import except_all  # noqa
from .expression import exists  # noqa
from .expression import extract  # noqa
from .expression import false  # noqa
from .expression import False_  # noqa
from .expression import FromClause  # noqa
from .expression import func  # noqa
from .expression import funcfilter  # noqa
from .expression import Insert  # noqa
from .expression import insert  # noqa
from .expression import intersect  # noqa
from .expression import intersect_all  # noqa
from .expression import Join  # noqa
from .expression import join  # noqa
from .expression import label  # noqa
from .expression import lateral  # noqa
from .expression import literal  # noqa
from .expression import literal_column  # noqa
from .expression import modifier  # noqa
from .expression import not_  # noqa
from .expression import null  # noqa
from .expression import nullsfirst  # noqa
from .expression import nullslast  # noqa
from .expression import or_  # noqa
from .expression import outerjoin  # noqa
from .expression import outparam  # noqa
from .expression import over  # noqa
from .expression import quoted_name  # noqa
from .expression import Select  # noqa
from .expression import select  # noqa
from .expression import Selectable  # noqa
from .expression import subquery  # noqa
from .expression import table  # noqa
from .expression import TableClause  # noqa
from .expression import TableSample  # noqa
from .expression import tablesample  # noqa
from .expression import text  # noqa
from .expression import true  # noqa
from .expression import True_  # noqa
from .expression import tuple_  # noqa
from .expression import type_coerce  # noqa
from .expression import union  # noqa
from .expression import union_all  # noqa
from .expression import Update  # noqa
from .expression import update  # noqa
from .expression import within_group  # noqa
from .visitors import ClauseVisitor  # noqa


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
    from .annotation import Annotated  # noqa
    from .elements import AnnotatedColumnElement
    from .elements import ClauseList  # noqa
    from .selectable import AnnotatedFromClause  # noqa

    _prepare_annotations(ColumnElement, AnnotatedColumnElement)
    _prepare_annotations(FromClause, AnnotatedFromClause)
    _prepare_annotations(ClauseList, Annotated)

    _sa_util.dependencies.resolve_all("sqlalchemy.sql")

    from . import naming  # noqa


__go(locals())
