# sql/__init__.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .expression import (
    Alias,
    ClauseElement,
    ColumnCollection,
    ColumnElement,
    CompoundSelect,
    Delete,
    FromClause,
    Insert,
    Join,
    Select,
    Selectable,
    TableClause,
    Update,
    alias,
    and_,
    asc,
    between,
    bindparam,
    case,
    cast,
    collate,
    column,
    delete,
    desc,
    distinct,
    except_,
    except_all,
    exists,
    extract,
    false,
    False_,
    func,
    insert,
    intersect,
    intersect_all,
    join,
    label,
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
    table,
    text,
    true,
    True_,
    tuple_,
    type_coerce,
    union,
    union_all,
    update,
    )

from .visitors import ClauseVisitor


def __go(lcls):
    global __all__
    from .. import util as _sa_util

    import inspect as _inspect

    __all__ = sorted(name for name, obj in lcls.items()
                 if not (name.startswith('_') or _inspect.ismodule(obj)))

    from .annotation import _prepare_annotations, Annotated
    from .elements import AnnotatedColumnElement, ClauseList
    from .selectable import AnnotatedFromClause
    _prepare_annotations(ColumnElement, AnnotatedColumnElement)
    _prepare_annotations(FromClause, AnnotatedFromClause)
    _prepare_annotations(ClauseList, Annotated)

    _sa_util.dependencies.resolve_all("sqlalchemy.sql")

__go(locals())

