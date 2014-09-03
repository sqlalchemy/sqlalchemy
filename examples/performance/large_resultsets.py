"""In this series of tests, we are looking at time to load 1M very small
and simple rows.

"""
from . import Profiler

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine, literal_column
from sqlalchemy.orm import Session, Bundle

Base = declarative_base()
engine = None


class Customer(Base):
    __tablename__ = "customer"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    description = Column(String(255))


def setup_database(dburl, echo, num):
    global engine
    engine = create_engine(dburl, echo=echo)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    s = Session(engine)
    for chunk in range(0, num, 10000):
        s.bulk_insert_mappings(Customer, [
            {
                'name': 'customer name %d' % i,
                'description': 'customer description %d' % i
            } for i in range(chunk, chunk + 10000)
        ])
    s.commit()


@Profiler.profile
def test_orm_full_objects(n):
    """Load fully tracked objects using the ORM."""

    sess = Session(engine)
    # avoid using all() so that we don't have the overhead of building
    # a large list of full objects in memory
    for obj in sess.query(Customer).yield_per(1000).limit(n):
        pass


@Profiler.profile
def test_orm_bundles(n):
    """Load lightweight "bundle" objects using the ORM."""

    sess = Session(engine)
    bundle = Bundle('customer',
                    Customer.id, Customer.name, Customer.description)
    for row in sess.query(bundle).yield_per(10000).limit(n):
        pass


@Profiler.profile
def test_orm_columns(n):
    """Load individual columns into named tuples using the ORM."""

    sess = Session(engine)
    for row in sess.query(
        Customer.id, Customer.name,
            Customer.description).yield_per(10000).limit(n):
        pass


@Profiler.profile
def test_core_fetchall(n):
    """Load Core result rows using Core / fetchall."""

    with engine.connect() as conn:
        result = conn.execute(Customer.__table__.select().limit(n)).fetchall()
        for row in result:
            data = row['id'], row['name'], row['description']


@Profiler.profile
def test_core_fetchchunks_w_streaming(n):
    """Load Core result rows using Core with fetchmany and
    streaming results."""

    with engine.connect() as conn:
        result = conn.execution_options(stream_results=True).\
            execute(Customer.__table__.select().limit(n))
        while True:
            chunk = result.fetchmany(10000)
            if not chunk:
                break
            for row in chunk:
                data = row['id'], row['name'], row['description']


@Profiler.profile
def test_core_fetchchunks(n):
    """Load Core result rows using Core / fetchmany."""

    with engine.connect() as conn:
        result = conn.execute(Customer.__table__.select().limit(n))
        while True:
            chunk = result.fetchmany(10000)
            if not chunk:
                break
            for row in chunk:
                data = row['id'], row['name'], row['description']


@Profiler.profile
def test_dbapi_fetchall(n):
    """Load DBAPI cursor rows using fetchall()"""

    _test_dbapi_raw(n, True)


@Profiler.profile
def test_dbapi_fetchchunks(n):
    """Load DBAPI cursor rows using fetchmany()
    (usually doesn't limit memory)"""

    _test_dbapi_raw(n, False)


def _test_dbapi_raw(n, fetchall):
    compiled = Customer.__table__.select().limit(n).\
        compile(
            dialect=engine.dialect,
            compile_kwargs={"literal_binds": True})

    sql = str(compiled)

    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(sql)

    if fetchall:
        for row in cursor.fetchall():
            # ensure that we fully fetch!
            data = row[0], row[1], row[2]
    else:
        while True:
            chunk = cursor.fetchmany(10000)
            if not chunk:
                break
            for row in chunk:
                data = row[0], row[1], row[2]
    conn.close()

if __name__ == '__main__':
    Profiler.main(setup_once=setup_database, num=1000000)
