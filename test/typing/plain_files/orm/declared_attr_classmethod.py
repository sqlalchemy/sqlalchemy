# Regression tests for the declared_attr typing issue reported in #9213.

import typing
from typing import Optional
from typing import assert_type

import sqlalchemy as sa
from sqlalchemy import Index
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class _Rhyme:
    __tablename__: str

    ph1: Mapped[int]
    ph2: Mapped[Optional[int]]

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> tuple[Index, ...]:
        return (sa.Index(cls.__tablename__ + "_ph1_ph2", cls.ph1, cls.ph2),)


class Rhyme(_Rhyme, Base):
    __tablename__ = "rhyme"
    id: Mapped[int] = mapped_column(primary_key=True)


if typing.TYPE_CHECKING:
    assert_type(Rhyme.__tablename__, str)
