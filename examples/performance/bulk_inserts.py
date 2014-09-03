from . import Profiler

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine, bindparam
from sqlalchemy.orm import Session

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


@Profiler.profile
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


@Profiler.profile
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


@Profiler.profile
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


@Profiler.profile
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


@Profiler.profile
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


@Profiler.profile
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


@Profiler.profile
def test_dbapi_raw(n):
    """The DBAPI's pure C API inserting rows in bulk, no pure Python at all"""

    conn = engine.pool._creator()
    cursor = conn.cursor()
    compiled = Customer.__table__.insert().values(
        name=bindparam('name'),
        description=bindparam('description')).\
        compile(dialect=engine.dialect)

    if compiled.positional:
        args = (
            ('customer name %d' % i, 'customer description %d' % i)
            for i in range(n))
    else:
        args = (
            dict(
                name='customer name %d' % i,
                description='customer description %d' % i
            )
            for i in range(n)
        )

    cursor.executemany(
        str(compiled),
        list(args)
    )
    conn.commit()
    conn.close()

if __name__ == '__main__':
    Profiler.main(setup=setup_database, num=100000)
