"""In this series of tests, we are looking at time to load a large number
of very small and simple rows.

A special test here illustrates the difference between fetching the
rows from the raw DBAPI and throwing them away, vs. assembling each
row into a completely basic Python object and appending to a list. The
time spent typically more than doubles.  The point is that while
DBAPIs will give you raw rows very fast if they are written in C, the
moment you do anything with those rows, even something trivial,
overhead grows extremely fast in cPython. SQLAlchemy's Core and
lighter-weight ORM options add absolutely minimal overhead, and the
full blown ORM doesn't do terribly either even though mapped objects
provide a huge amount of functionality.

"""
from . import Profiler

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import Session, Bundle

Base = declarative_base()
engine = None


class Customer(Base):
    __tablename__ = "customer"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    description = Column(String(255))


Profiler.init("large_resultsets", num=500000)


@Profiler.setup_once
def setup_database(dburl, echo, num):
    global engine
    engine = create_engine(dburl, echo=echo)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    s = Session(engine)
    for chunk in range(0, num, 10000):
        s.execute(
            Customer.__table__.insert(),
            params=[
                {
                    'name': 'customer name %d' % i,
                    'description': 'customer description %d' % i
                } for i in range(chunk, chunk + 10000)])
    s.commit()


@Profiler.profile
def test_orm_full_objects_list(n):
    """Load fully tracked ORM objects into one big list()."""

    sess = Session(engine)
    objects = list(sess.query(Customer).limit(n))


@Profiler.profile
def test_orm_full_objects_chunks(n):
    """Load fully tracked ORM objects a chunk at a time using yield_per()."""

    sess = Session(engine)
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
    """Load Core result rows using fetchall."""

    with engine.connect() as conn:
        result = conn.execute(Customer.__table__.select().limit(n)).fetchall()
        for row in result:
            data = row['id'], row['name'], row['description']


@Profiler.profile
def test_core_fetchmany_w_streaming(n):
    """Load Core result rows using fetchmany/streaming."""

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
def test_core_fetchmany(n):
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
def test_dbapi_fetchall_plus_append_objects(n):
    """Load rows using DBAPI fetchall(), generate an object for each row."""

    _test_dbapi_raw(n, True)


@Profiler.profile
def test_dbapi_fetchall_no_object(n):
    """Load rows using DBAPI fetchall(), don't make any objects."""

    _test_dbapi_raw(n, False)


def _test_dbapi_raw(n, make_objects):
    compiled = Customer.__table__.select().limit(n).\
        compile(
            dialect=engine.dialect,
            compile_kwargs={"literal_binds": True})

    if make_objects:
        # because if you're going to roll your own, you're probably
        # going to do this, so see how this pushes you right back into
        # ORM land anyway :)
        class SimpleCustomer(object):
            def __init__(self, id, name, description):
                self.id = id
                self.name = name
                self.description = description

    sql = str(compiled)

    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.execute(sql)

    if make_objects:
        for row in cursor.fetchall():
            # ensure that we fully fetch!
            customer = SimpleCustomer(
                id=row[0], name=row[1], description=row[2])
    else:
        for row in cursor.fetchall():
            # ensure that we fully fetch!
            data = row[0], row[1], row[2]

    conn.close()

if __name__ == '__main__':
    Profiler.main()
