# sqlalchemy/__init__.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php



__version__ = "1.2.16"


def __go(lcls):
    global __all__

    from . import events
    from . import util as _sa_util

    import inspect as _inspect

    __all__ = sorted(
        name
        for name, obj in lcls.items()
        if not (name.startswith("_") or _inspect.ismodule(obj))
    )

    _sa_util.dependencies.resolve_all("sqlalchemy")


__go(locals())
