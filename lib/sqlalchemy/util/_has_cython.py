# util/_has_cython.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

import typing


def _all_cython_modules():
    """Returns all modules that can be compiled using cython.
    Call ``_is_compiled()`` to check if the module is compiled or not.
    """
    from . import _collections_cy
    from . import _immutabledict_cy
    from ..engine import _processors_cy
    from ..engine import _result_cy
    from ..engine import _row_cy
    from ..engine import _util_cy as engine_util
    from ..sql import _util_cy as sql_util

    return (
        _collections_cy,
        _immutabledict_cy,
        _processors_cy,
        _result_cy,
        _row_cy,
        engine_util,
        sql_util,
    )


_CYEXTENSION_MSG: str
if not typing.TYPE_CHECKING:
    HAS_CYEXTENSION = all(m._is_compiled() for m in _all_cython_modules())
    if HAS_CYEXTENSION:
        _CYEXTENSION_MSG = "Loaded"
    else:
        _CYEXTENSION_MSG = ", ".join(
            m.__name__ for m in _all_cython_modules() if not m._is_compiled()
        )
        _CYEXTENSION_MSG = f"Modules {_CYEXTENSION_MSG} are not compiled"
else:
    HAS_CYEXTENSION = False
