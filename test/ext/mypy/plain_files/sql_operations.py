import typing

from sqlalchemy import column
from sqlalchemy import Integer

# builtin.pyi stubs define object.__eq__() as returning bool,  which
# can't be overridden (it's final).  So for us to type `__eq__()` and
# `__ne__()`, we have to use type: ignore[override].  Test if does this mean
# the typing tools don't know the type, or if they just ignore the error.
# (it's fortunately the former)
expr1 = column("x", Integer) == 10


if typing.TYPE_CHECKING:

    # as far as if this is ColumnElement, BinaryElement, SQLCoreOperations,
    # that might change.  main thing is it's SomeSQLColThing[bool] and
    # not 'bool' or 'Any'.
    # EXPECTED_TYPE: sqlalchemy..*ColumnElement\[builtins.bool\]
    reveal_type(expr1)
