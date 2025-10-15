import typing

from sqlalchemy import create_engine
from sqlalchemy import inspect


e = create_engine("sqlite://")

insp = inspect(e)

cols = insp.get_columns("some_table")

c1 = cols[0]

if typing.TYPE_CHECKING:
    # EXPECTED_TYPE: Engine
    reveal_type(e)

    # EXPECTED_TYPE: Inspector
    reveal_type(insp)

    # EXPECTED_TYPE: list[TypedDict(.*ReflectedColumn.*)]
    reveal_type(cols)
