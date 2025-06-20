from typing import Any

from sqlalchemy import JSON
from sqlalchemy import Select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.sql.compiler import SQLCompiler


@compiles(Select[Any], "my_cool_driver")
def go(sel: Select[Any], compiler: SQLCompiler, **kw: Any) -> str:
    return "select 42"


MutableList.as_mutable(JSON)
MutableDict.as_mutable(JSON())
