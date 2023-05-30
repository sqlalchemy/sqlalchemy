import typing

from sqlalchemy import create_engine
from sqlalchemy import inspect


e = create_engine("sqlite://")

insp = inspect(e)

cols = insp.get_columns("some_table")

c1 = cols[0]

if typing.TYPE_CHECKING:
    # EXPECTED_RE_TYPE: sqlalchemy.engine.base.Engine
    reveal_type(e)

    # EXPECTED_RE_TYPE: sqlalchemy.engine.reflection.Inspector.*
    reveal_type(insp)

    # EXPECTED_RE_TYPE: .*list.*TypedDict.*ReflectedColumn.*
    reveal_type(cols)
