import typing

from sqlalchemy import Boolean
from sqlalchemy import column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String


# builtin.pyi stubs define object.__eq__() as returning bool,  which
# can't be overridden (it's final).  So for us to type `__eq__()` and
# `__ne__()`, we have to use type: ignore[override].  Test if does this mean
# the typing tools don't know the type, or if they just ignore the error.
# (it's fortunately the former)
expr1 = column("x", Integer) == 10

c1 = column("a", String)

c2 = column("a", Integer)

expr2 = c2.in_([1, 2, 3])

expr3 = c2 / 5

expr4 = -c2

expr5 = ~(c2 == 5)

q = column("q", Boolean)
expr6 = ~q

expr7 = c1 + "x"

expr8 = c2 + 10

stmt = select(column("q")).where(lambda: column("g") > 5).where(c2 == 5)

expr9 = c1.bool_op("@@")(func.to_tsquery("some & query"))


if typing.TYPE_CHECKING:

    # as far as if this is ColumnElement, BinaryElement, SQLCoreOperations,
    # that might change.  main thing is it's SomeSQLColThing[bool] and
    # not 'bool' or 'Any'.
    # EXPECTED_RE_TYPE: sqlalchemy..*ColumnElement\[builtins.bool\]
    reveal_type(expr1)

    # EXPECTED_RE_TYPE: sqlalchemy..*ColumnClause\[builtins.str.?\]
    reveal_type(c1)

    # EXPECTED_RE_TYPE: sqlalchemy..*ColumnClause\[builtins.int.?\]
    reveal_type(c2)

    # EXPECTED_RE_TYPE: sqlalchemy..*BinaryExpression\[builtins.bool\]
    reveal_type(expr2)

    # EXPECTED_RE_TYPE: sqlalchemy..*ColumnElement\[Union\[builtins.float, .*\.Decimal\]\]
    reveal_type(expr3)

    # EXPECTED_RE_TYPE: sqlalchemy..*UnaryExpression\[builtins.int.?\]
    reveal_type(expr4)

    # EXPECTED_RE_TYPE: sqlalchemy..*ColumnElement\[builtins.bool.?\]
    reveal_type(expr5)

    # EXPECTED_RE_TYPE: sqlalchemy..*ColumnElement\[builtins.bool.?\]
    reveal_type(expr6)

    # EXPECTED_RE_TYPE: sqlalchemy..*ColumnElement\[builtins.str\]
    reveal_type(expr7)

    # EXPECTED_RE_TYPE: sqlalchemy..*ColumnElement\[builtins.int.?\]
    reveal_type(expr8)

    # EXPECTED_TYPE: BinaryExpression[bool]
    reveal_type(expr9)
