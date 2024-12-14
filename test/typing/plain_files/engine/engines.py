from sqlalchemy import create_engine
from sqlalchemy import Pool
from sqlalchemy import select
from sqlalchemy import text


def regular() -> None:
    e = create_engine("sqlite://")

    # EXPECTED_TYPE: Engine
    reveal_type(e)

    with e.connect() as conn:
        # EXPECTED_TYPE: Connection
        reveal_type(conn)

        result = conn.execute(text("select * from table"))

        # EXPECTED_TYPE: CursorResult[Unpack[.*tuple[Any, ...]]]
        reveal_type(result)

    with e.begin() as conn:
        # EXPECTED_TYPE: Connection
        reveal_type(conn)

        result = conn.execute(text("select * from table"))

        # EXPECTED_TYPE: CursorResult[Unpack[.*tuple[Any, ...]]]
        reveal_type(result)

    engine = create_engine("postgresql://scott:tiger@localhost/test")
    status: str = engine.pool.status()
    other_pool: Pool = engine.pool.recreate()
    ce = select(1).compile(e)
    ce.statement
    cc = select(1).compile(conn)
    cc.statement

    print(status, other_pool)
