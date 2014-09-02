import time

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import Session

Base = declarative_base()
engine = None


class Customer(Base):
    __tablename__ = "customer"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    description = Column(String(255))


def setup_database():
    global engine
    engine = create_engine("sqlite:///insert_speed.db", echo=False)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

_tests = []


def _test(fn):
    _tests.append(fn)
    return fn


@_test
def test_flush_no_pk(n):
    """Individual INSERT statements via the ORM, calling upon last row id"""
    session = Session(bind=engine)
    for chunk in range(0, n, 1000):
        session.add_all([
            Customer(
                name='customer name %d' % i,
                description='customer description %d' % i)
            for i in range(chunk, chunk + 1000)
        ])
        session.flush()
    session.commit()


@_test
def test_bulk_save_return_pks(n):
    """Individual INSERT statements in "bulk", but calling upon last row id"""
    session = Session(bind=engine)
    session.bulk_save_objects([
        Customer(
            name='customer name %d' % i,
            description='customer description %d' % i
        )
        for i in range(n)
    ], return_defaults=True)
    session.commit()


@_test
def test_flush_pk_given(n):
    """Batched INSERT statements via the ORM, PKs already defined"""
    session = Session(bind=engine)
    for chunk in range(0, n, 1000):
        session.add_all([
            Customer(
                id=i + 1,
                name='customer name %d' % i,
                description='customer description %d' % i)
            for i in range(chunk, chunk + 1000)
        ])
        session.flush()
    session.commit()


@_test
def test_bulk_save(n):
    """Batched INSERT statements via the ORM in "bulk", discarding PK values."""
    session = Session(bind=engine)
    session.bulk_save_objects([
        Customer(
            name='customer name %d' % i,
            description='customer description %d' % i
        )
        for i in range(n)
    ])
    session.commit()


@_test
def test_bulk_insert_mappings(n):
    """Batched INSERT statements via the ORM "bulk", using dictionaries instead of objects"""
    session = Session(bind=engine)
    session.bulk_insert_mappings(Customer, [
        dict(
            name='customer name %d' % i,
            description='customer description %d' % i
        )
        for i in range(n)
    ])
    session.commit()


@_test
def test_core_insert(n):
    """A single Core INSERT construct inserting mappings in bulk."""
    conn = engine.connect()
    conn.execute(
        Customer.__table__.insert(),
        [
            dict(
                name='customer name %d' % i,
                description='customer description %d' % i
            )
            for i in range(n)
        ])


@_test
def test_sqlite_raw(n):
    """pysqlite's pure C API inserting rows in bulk, no pure Python at all"""
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT INTO customer (name, description) VALUES(:name, :description)",
        [
            dict(
                name='customer name %d' % i,
                description='customer description %d' % i
            )
            for i in range(n)
        ]
    )
    conn.commit()


def run_tests(n):
    for fn in _tests:
        setup_database()
        now = time.time()
        fn(n)
        total = time.time() - now

        print("Test: %s; Total time %s" % (fn.__doc__, total))

if __name__ == '__main__':
    run_tests(100000)
