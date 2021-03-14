from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base

# this is actually in orm now


Base = declarative_base()


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


f2 = Foo(name="some name", other_name="some other name")
