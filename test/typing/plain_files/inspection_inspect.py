from typing import List

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import DeclarativeBaseNoMeta
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapper


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


# EXPECTED_TYPE: Mapper[Any]
reveal_type(A.__mapper__)
# EXPECTED_TYPE: Mapper[Any]
reveal_type(B.__mapper__)

a1 = A(data="d")
b1 = B(data="d")

e = create_engine("sqlite://")

insp_a1 = inspect(a1)

t: bool = insp_a1.transient
# EXPECTED_TYPE: InstanceState[A]
reveal_type(insp_a1)
# EXPECTED_TYPE: InstanceState[B]
reveal_type(inspect(b1))

m: Mapper[A] = inspect(A)
# EXPECTED_TYPE: Mapper[A]
reveal_type(inspect(A))
# EXPECTED_TYPE: Mapper[B]
reveal_type(inspect(B))

tables: List[str] = inspect(e).get_table_names()

i: Inspector = inspect(e)
# EXPECTED_TYPE: Inspector
reveal_type(inspect(e))


with e.connect() as conn:
    inspect(conn).get_table_names()
