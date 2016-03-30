"""This series of tests illustrates different ways to SELECT a single
record by primary key


"""
from . import Profiler

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, create_engine, \
    bindparam, select
from sqlalchemy.orm import Session, deferred
from sqlalchemy.ext import baked
import random

Base = declarative_base()
engine = None

ids = range(1, 11000)


class Customer(Base):
    __tablename__ = "customer"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    description = Column(String(255))
    q = Column(Integer)
    p = Column(Integer)
    x = deferred(Column(Integer))
    y = deferred(Column(Integer))
    z = deferred(Column(Integer))

Profiler.init("short_selects", num=10000)


@Profiler.setup
def setup_database(dburl, echo, num):
    global engine
    engine = create_engine(dburl, echo=echo)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    sess = Session(engine)
    sess.add_all([
        Customer(
            id=i, name='c%d' % i, description="c%d" % i,
            q=i * 10,
            p=i * 20,
            x=i * 30,
            y=i * 40,
        )
        for i in ids
    ])
    sess.commit()


@Profiler.profile
def test_orm_query(n):
    """test a straight ORM query of the full entity."""
    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        session.query(Customer).filter(Customer.id == id_).one()


@Profiler.profile
def test_orm_query_cols_only(n):
    """test an ORM query of only the entity columns."""
    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        session.query(
            Customer.id, Customer.name, Customer.description
        ).filter(Customer.id == id_).one()


@Profiler.profile
def test_baked_query(n):
    """test a baked query of the full entity."""
    bakery = baked.bakery()
    s = Session(bind=engine)
    for id_ in random.sample(ids, n):
        q = bakery(lambda s: s.query(Customer))
        q += lambda q: q.filter(Customer.id == bindparam('id'))
        q(s).params(id=id_).one()


@Profiler.profile
def test_baked_query_cols_only(n):
    """test a baked query of only the entity columns."""
    bakery = baked.bakery()
    s = Session(bind=engine)
    for id_ in random.sample(ids, n):
        q = bakery(
            lambda s: s.query(
                Customer.id, Customer.name, Customer.description))
        q += lambda q: q.filter(Customer.id == bindparam('id'))
        q(s).params(id=id_).one()


@Profiler.profile
def test_core_new_stmt_each_time(n):
    """test core, creating a new statement each time."""

    with engine.connect() as conn:
        for id_ in random.sample(ids, n):
            stmt = select([Customer.__table__]).where(Customer.id == id_)
            row = conn.execute(stmt).first()
            tuple(row)


@Profiler.profile
def test_core_reuse_stmt(n):
    """test core, reusing the same statement (but recompiling each time)."""

    stmt = select([Customer.__table__]).where(Customer.id == bindparam('id'))
    with engine.connect() as conn:
        for id_ in random.sample(ids, n):

            row = conn.execute(stmt, id=id_).first()
            tuple(row)


@Profiler.profile
def test_core_reuse_stmt_compiled_cache(n):
    """test core, reusing the same statement + compiled cache."""

    compiled_cache = {}
    stmt = select([Customer.__table__]).where(Customer.id == bindparam('id'))
    with engine.connect().\
            execution_options(compiled_cache=compiled_cache) as conn:
        for id_ in random.sample(ids, n):
            row = conn.execute(stmt, id=id_).first()
            tuple(row)


if __name__ == '__main__':
    Profiler.main()
