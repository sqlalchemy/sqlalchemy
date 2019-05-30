"""In this series of tests, we're looking at a method that inserts a row
within a distinct transaction, and afterwards returns to essentially a
"closed" state.   This would be analogous to an API call that starts up
a database connection, inserts the row, commits and closes.

"""
from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import pool
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from . import Profiler


Base = declarative_base()
engine = None


class Customer(Base):
    __tablename__ = "customer"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    description = Column(String(255))


Profiler.init("single_inserts", num=10000)


@Profiler.setup
def setup_database(dburl, echo, num):
    global engine
    engine = create_engine(dburl, echo=echo)
    if engine.dialect.name == "sqlite":
        engine.pool = pool.StaticPool(creator=engine.pool._creator)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


@Profiler.profile
def test_orm_commit(n):
    """Individual INSERT/COMMIT pairs via the ORM"""

    for i in range(n):
        session = Session(bind=engine)
        session.add(
            Customer(
                name="customer name %d" % i,
                description="customer description %d" % i,
            )
        )
        session.commit()


@Profiler.profile
def test_bulk_save(n):
    """Individual INSERT/COMMIT pairs using the "bulk" API """

    for i in range(n):
        session = Session(bind=engine)
        session.bulk_save_objects(
            [
                Customer(
                    name="customer name %d" % i,
                    description="customer description %d" % i,
                )
            ]
        )
        session.commit()


@Profiler.profile
def test_bulk_insert_dictionaries(n):
    """Individual INSERT/COMMIT pairs using the "bulk" API with dictionaries"""

    for i in range(n):
        session = Session(bind=engine)
        session.bulk_insert_mappings(
            Customer,
            [
                dict(
                    name="customer name %d" % i,
                    description="customer description %d" % i,
                )
            ],
        )
        session.commit()


@Profiler.profile
def test_core(n):
    """Individual INSERT/COMMIT pairs using Core."""

    for i in range(n):
        with engine.begin() as conn:
            conn.execute(
                Customer.__table__.insert(),
                dict(
                    name="customer name %d" % i,
                    description="customer description %d" % i,
                ),
            )


@Profiler.profile
def test_core_query_caching(n):
    """Individual INSERT/COMMIT pairs using Core with query caching"""

    cache = {}
    ins = Customer.__table__.insert()
    for i in range(n):
        with engine.begin() as conn:
            conn.execution_options(compiled_cache=cache).execute(
                ins,
                dict(
                    name="customer name %d" % i,
                    description="customer description %d" % i,
                ),
            )


@Profiler.profile
def test_dbapi_raw_w_connect(n):
    """Individual INSERT/COMMIT pairs w/ DBAPI + connection each time"""

    _test_dbapi_raw(n, True)


@Profiler.profile
def test_dbapi_raw_w_pool(n):
    """Individual INSERT/COMMIT pairs w/ DBAPI + connection pool"""

    _test_dbapi_raw(n, False)


def _test_dbapi_raw(n, connect):
    compiled = (
        Customer.__table__.insert()
        .values(name=bindparam("name"), description=bindparam("description"))
        .compile(dialect=engine.dialect)
    )

    if compiled.positional:
        args = (
            ("customer name %d" % i, "customer description %d" % i)
            for i in range(n)
        )
    else:
        args = (
            dict(
                name="customer name %d" % i,
                description="customer description %d" % i,
            )
            for i in range(n)
        )
    sql = str(compiled)

    if connect:
        for arg in args:
            # there's no connection pool, so if these were distinct
            # calls, we'd be connecting each time
            conn = engine.pool._creator()
            cursor = conn.cursor()
            cursor.execute(sql, arg)
            cursor.lastrowid
            conn.commit()
            conn.close()
    else:
        for arg in args:
            conn = engine.raw_connection()
            cursor = conn.cursor()
            cursor.execute(sql, arg)
            cursor.lastrowid
            conn.commit()
            conn.close()


if __name__ == "__main__":
    Profiler.main()
