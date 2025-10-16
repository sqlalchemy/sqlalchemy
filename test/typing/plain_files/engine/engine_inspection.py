import typing
from typing import assert_type

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.engine import Inspector
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.interfaces import ReflectedColumn


e = create_engine("sqlite://")

insp = inspect(e)

cols = insp.get_columns("some_table")

c1 = cols[0]

if typing.TYPE_CHECKING:
    assert_type(e, Engine)

    assert_type(insp, Inspector)

    assert_type(cols, list[ReflectedColumn])
