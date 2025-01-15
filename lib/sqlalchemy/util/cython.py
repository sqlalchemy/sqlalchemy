# util/cython.py
# Copyright (C) 2005-2025 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Type
from typing import TypeVar

_T = TypeVar("_T")
_NO_OP = Callable[[_T], _T]

# cython module shims
# --
IS_SHIM = True
# constants
compiled = False

# types
int = int  # noqa: A001
bint = bool
longlong = int
ulonglong = int
Py_ssize_t = int
uint = int
float = float  # noqa: A001
double = float
void = Any


# functions
def _no_op(fn: _T) -> _T:
    return fn


cclass = _no_op  # equivalent to "cdef class"
ccall = _no_op  # equivalent to "cpdef" function
cfunc = _no_op  # equivalent to "cdef" function
inline = _no_op
final = _no_op
pointer = _no_op  # not sure how to express a pointer to a type


def declare(t: Type[_T], value: Any = None, **kw: Any) -> _T:
    return value  # type: ignore[no-any-return]


def annotation_typing(_: bool) -> _NO_OP[_T]:
    return _no_op


def exceptval(value: Any = None, *, check: bool = False) -> _NO_OP[_T]:
    return _no_op


def cast(type_: Type[_T], value: Any, *, typecheck: bool = False) -> _T:
    return value  # type: ignore[no-any-return]
