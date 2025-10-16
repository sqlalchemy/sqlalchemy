from typing import Any
from typing import assert_type
from typing import List

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import DeclarativeBaseNoMeta
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapper
from sqlalchemy.orm.state import InstanceState


class Base(DeclarativeBase):
    pass


class A(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True)
    data: Mapped[str]


class BaseNoMeta(DeclarativeBaseNoMeta):
    pass


class B(BaseNoMeta):
    __tablename__ = "b"

    id: Mapped[int] = mapped_column(primary_key=True)
    data: Mapped[str]


assert_type(A.__mapper__, Mapper[Any])
assert_type(B.__mapper__, Mapper[Any])

a1 = A(data="d")
b1 = B(data="d")

e = create_engine("sqlite://")

insp_a1 = inspect(a1)

t: bool = insp_a1.transient
assert_type(insp_a1, InstanceState[A])
assert_type(inspect(b1), InstanceState[B])

m: Mapper[A] = inspect(A)
assert_type(inspect(A), Mapper[A])
assert_type(inspect(B), Mapper[B])

tables: List[str] = inspect(e).get_table_names()

i: Inspector = inspect(e)
assert_type(inspect(e), Inspector)


with e.connect() as conn:
    inspect(conn).get_table_names()
