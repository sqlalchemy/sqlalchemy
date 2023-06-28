from sqlalchemy import column
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
