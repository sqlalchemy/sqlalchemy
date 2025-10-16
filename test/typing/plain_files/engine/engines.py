from typing import Any
from typing import assert_type
from typing import Unpack

from sqlalchemy import Connection
from sqlalchemy import create_engine
from sqlalchemy import Pool
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.cursor import CursorResult


def regular() -> None:
    e = create_engine("sqlite://")

    assert_type(e, Engine)

    with e.connect() as conn:
        assert_type(conn, Connection)

        result = conn.execute(text("select * from table"))

        assert_type(result, CursorResult[Unpack[tuple[Any, ...]]])

    with e.begin() as conn:
        assert_type(conn, Connection)

        result = conn.execute(text("select * from table"))

        assert_type(result, CursorResult[Unpack[tuple[Any, ...]]])

    engine = create_engine("postgresql://scott:tiger@localhost/test")
    status: str = engine.pool.status()
    other_pool: Pool = engine.pool.recreate()
    ce = select(1).compile(e)
    ce.statement
    cc = select(1).compile(conn)
    cc.statement

    print(status, other_pool)
