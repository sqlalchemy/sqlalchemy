from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import registry


reg: registry = registry()

# TODO: also reg.as_declarative_base()
Base = declarative_base()


class HasUpdatedAt:
    updated_at = Column(Integer)


@reg.mapped
class Foo(HasUpdatedAt):
    __tablename__ = "foo"
    id: int = Column(Integer(), primary_key=True)
    name: str = Column(String)


class Bar(HasUpdatedAt, Base):
    __tablename__ = "bar"
    id = Column(Integer(), primary_key=True)
    num = Column(Integer)


Foo.updated_at.in_([1, 2, 3])
Bar.updated_at.in_([1, 2, 3])

f1 = Foo(name="name", updated_at=5)

b1 = Bar(num=5, updated_at=6)


# test that we detected this as an unmapped mixin
# EXPECTED_MYPY: Unexpected keyword argument "updated_at" for "HasUpdatedAt"
HasUpdatedAt(updated_at=5)
