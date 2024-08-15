"""This series of tests illustrates different ways to SELECT a single
record by primary key


"""
from __future__ import annotations

import random
from typing import Dict
from typing import Optional
from typing import TYPE_CHECKING

from sqlalchemy import bindparam
from sqlalchemy import create_engine
from sqlalchemy import Identity
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.ext import baked
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select as future_select
from sqlalchemy.orm import deferred
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Session
from sqlalchemy.sql import lambdas
from . import Profiler

if TYPE_CHECKING:
    from sqlalchemy import Engine


Base = declarative_base()
engine: Optional[Engine] = None

ids = range(1, 11000)


class Customer(Base):
    __tablename__ = "customer"
    id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str] = mapped_column(String(255))
    q: Mapped[int] = mapped_column(Integer)
    p: Mapped[int] = mapped_column(Integer)
    x: Mapped[int] = deferred(mapped_column(Integer))
    y: Mapped[int] = deferred(mapped_column(Integer))
    z: Mapped[int] = deferred(mapped_column(Integer))


Profiler.init("short_selects", num=10000)


@Profiler.setup
def setup_database(dburl: str, echo: bool, num: int):
    global engine
    engine = create_engine(dburl, echo=echo)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    sess = Session(engine)
    sess.add_all(
        [
            Customer(
                id=i,
                name="c%d" % i,
                description="c%d" % i,
                q=i * 10,
                p=i * 20,
                x=i * 30,
                y=i * 40,
            )
            for i in ids
        ]
    )
    sess.commit()


@Profiler.profile
def test_orm_query_classic_style(n: int) -> None:
    """classic ORM query of the full entity."""
    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        session.query(Customer).filter(Customer.id == id_).one()


@Profiler.profile
def test_orm_query_new_style(n: int) -> None:
    """new style ORM select() of the full entity."""

    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        stmt = future_select(Customer).where(Customer.id == id_)
        session.execute(stmt).scalar_one()


@Profiler.profile
def test_orm_query_new_style_using_embedded_lambdas(n: int) -> None:
    """new style ORM select() of the full entity w/ embedded lambdas."""
    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        stmt = future_select(lambda: Customer).where(
            lambda: Customer.id == id_
        )
        session.execute(stmt).scalar_one()


@Profiler.profile
def test_orm_query_new_style_using_external_lambdas(n: int) -> None:
    """new style ORM select() of the full entity w/ external lambdas."""

    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        stmt = lambdas.lambda_stmt(lambda: future_select(Customer))
        stmt += lambda s: s.where(Customer.id == id_)
        session.execute(stmt).scalar_one()


@Profiler.profile
def test_orm_query_classic_style_cols_only(n: int) -> None:
    """classic ORM query against columns"""
    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        session.query(Customer.id, Customer.name, Customer.description).filter(
            Customer.id == id_
        ).one()


@Profiler.profile
def test_orm_query_new_style_ext_lambdas_cols_only(n: int) -> None:
    """new style ORM query w/ external lambdas against columns."""
    s = Session(bind=engine)
    for id_ in random.sample(ids, n):
        stmt = lambdas.lambda_stmt(
            lambda: future_select(
                Customer.id, Customer.name, Customer.description
            )
        ) + (lambda s: s.filter(Customer.id == id_))
        s.execute(stmt).one()


@Profiler.profile
def test_baked_query(n: int) -> None:
    """test a baked query of the full entity."""
    bakery = baked.bakery()
    s = Session(bind=engine)
    for id_ in random.sample(ids, n):
        q = bakery(lambda s: s.query(Customer))
        q += lambda q: q.filter(Customer.id == bindparam("id"))
        q(s).params(id=id_).one()


@Profiler.profile
def test_baked_query_cols_only(n: int) -> None:
    """test a baked query of only the entity columns."""
    bakery = baked.bakery()
    s = Session(bind=engine)
    for id_ in random.sample(ids, n):
        q = bakery(
            lambda s: s.query(Customer.id, Customer.name, Customer.description)
        )
        q += lambda q: q.filter(Customer.id == bindparam("id"))
        q(s).params(id=id_).one()


@Profiler.profile
def test_core_new_stmt_each_time(n: int) -> None:
    """test core, creating a new statement each time."""
    if TYPE_CHECKING:
        assert engine is not None
    with engine.connect() as conn:
        for id_ in random.sample(ids, n):
            stmt = select(Customer.__table__).where(Customer.id == id_)
            row = conn.execute(stmt).first()
            tuple(row)


@Profiler.profile
def test_core_new_stmt_each_time_compiled_cache(n: int) -> None:
    """test core, creating a new statement each time, but using the cache."""
    if TYPE_CHECKING:
        assert engine is not None
    compiled_cache: Dict = {}
    with engine.connect().execution_options(
        compiled_cache=compiled_cache
    ) as conn:
        for id_ in random.sample(ids, n):
            stmt = select(Customer.__table__).where(Customer.id == id_)
            row = conn.execute(stmt).first()
            tuple(row)


@Profiler.profile
def test_core_reuse_stmt(n: int) -> None:
    """test core, reusing the same statement (but recompiling each time)."""
    if TYPE_CHECKING:
        assert engine is not None
    stmt = select(Customer.__table__).where(Customer.id == bindparam("id"))
    with engine.connect() as conn:
        for id_ in random.sample(ids, n):
            row = conn.execute(stmt, {"id": id_}).first()
            tuple(row)


@Profiler.profile
def test_core_reuse_stmt_compiled_cache(n: int) -> None:
    """test core, reusing the same statement + compiled cache."""
    if TYPE_CHECKING:
        assert engine is not None
    stmt = select(Customer.__table__).where(Customer.id == bindparam("id"))
    compiled_cache: Dict = {}
    with engine.connect().execution_options(
        compiled_cache=compiled_cache
    ) as conn:
        for id_ in random.sample(ids, n):
            row = conn.execute(stmt, {"id": id_}).first()
            tuple(row)


if __name__ == "__main__":
    Profiler.main()
