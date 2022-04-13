from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import registry
from sqlalchemy.orm.decl_api import DeclarativeMeta


class Base(metaclass=DeclarativeMeta):
    __abstract__ = True
    registry = registry()
    metadata = registry.metadata


class Foo(Base):
    __tablename__ = "foo"
    id: int = Column(Integer(), primary_key=True)
    name: str = Column(String)
    other_name: str = Column(String(50))


f1 = Foo()

val: int = f1.id

p: str = f1.name

Foo.id.property

# TODO: getitem checker?  this should raise
Foo.id.property_nonexistent
