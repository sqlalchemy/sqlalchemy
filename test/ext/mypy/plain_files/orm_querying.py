from __future__ import annotations

from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy.orm import aliased
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload


class Base(DeclarativeBase):
    pass


class A(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True)
    data: Mapped[str]
    bs: Mapped[list[B]] = relationship("B")


class B(Base):
    __tablename__ = "b"
    id: Mapped[int] = mapped_column(primary_key=True)
    a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
    data: Mapped[str]


def test_9669_and() -> None:
    select(A).options(selectinload(A.bs.and_(B.data == "some data")))


def test_9669_of_type() -> None:
    ba = aliased(B)
    select(A).options(selectinload(A.bs.of_type(ba)))
