# sql/__init__.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .expression import ColumnElement
from .expression import FromClause


def __go(lcls):
    global __all__
    from .. import util as _sa_util

    import inspect as _inspect

    __all__ = sorted(
        name
        for name, obj in lcls.items()
        if not (name.startswith("_") or _inspect.ismodule(obj))
    )

    from .annotation import _prepare_annotations, Annotated
    from .elements import AnnotatedColumnElement, ClauseList
    from .selectable import AnnotatedFromClause

    _prepare_annotations(ColumnElement, AnnotatedColumnElement)
    _prepare_annotations(FromClause, AnnotatedFromClause)
    _prepare_annotations(ClauseList, Annotated)

    _sa_util.dependencies.resolve_all("sqlalchemy.sql")

    from . import naming


__go(locals())
