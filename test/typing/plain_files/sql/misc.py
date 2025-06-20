from typing import Any

from sqlalchemy import column
from sqlalchemy import ColumnElement
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import table


def test_col_accessors() -> None:
    t = table("t", column("a"), column("b"), column("c"))

    t.c.a
    t.c["a"]

    t.c[2]
    t.c[0, 1]
    t.c[0, 1, "b", "c"]
    t.c[(0, 1, "b", "c")]

    t.c[:-1]
    t.c[0:2]


def test_col_get() -> None:
    col_id = column("id", Integer)
    col_alt = column("alt", Integer)
    tbl = table("mytable", col_id)

    # EXPECTED_TYPE: Union[ColumnClause[Any], None]
    reveal_type(tbl.c.get("id"))
    # EXPECTED_TYPE: Union[ColumnClause[Any], None]
    reveal_type(tbl.c.get("id", None))
    # EXPECTED_TYPE: Union[ColumnClause[Any], ColumnClause[int]]
    reveal_type(tbl.c.get("alt", col_alt))
    col: ColumnElement[Any] = tbl.c.get("foo", literal("bar"))
    print(col)
