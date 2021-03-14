from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import registry
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.schema import MetaData
from sqlalchemy.sql.schema import Table


Base = declarative_base()


class Foo(Base):
    __tablename__ = "foo"
    id: int = Column(Integer(), primary_key=True)
    name: str = Column(String)


class Bar(Foo):
    __tablename__ = "bar"
    id: int = Column(ForeignKey("foo.id"), primary_key=True)


class Bat(Foo):
    pass


m0: MetaData = Base.metadata
r0: registry = Base.registry

t1: Table = Foo.__table__
m1: MetaData = Foo.metadata

t2: Table = Bar.__table__
m2: MetaData = Bar.metadata

t3: Table = Bat.__table__
m3: MetaData = Bat.metadata
