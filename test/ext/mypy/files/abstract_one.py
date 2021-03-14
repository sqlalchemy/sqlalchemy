from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class FooBase(Base):
    __abstract__ = True

    updated_at = Column(Integer)


class Foo(FooBase):
    __tablename__ = "foo"
    id: int = Column(Integer(), primary_key=True)
    name: str = Column(String)


Foo.updated_at.in_([1, 2, 3])

f1 = Foo(name="name", updated_at=5)

# test that we read the __abstract__ flag and don't apply a constructor
# EXPECTED_MYPY: Unexpected keyword argument "updated_at" for "FooBase"
FooBase(updated_at=5)
