import typing
from typing import assert_type
from typing import Optional

from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class HasRelatedDataMixin:
    __tablename__: str

    ph1: Mapped[int]
    ph2: Mapped[Optional[int]]

    @declared_attr
    def related_data(cls) -> Mapped[str]:
        return mapped_column(Text(), deferred=True)

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> tuple[Index, ...]:
        return (Index(cls.__tablename__ + "_ph1_ph2", cls.ph1, cls.ph2),)


class User(HasRelatedDataMixin, Base):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return "user"

    @declared_attr.directive
    def __mapper_args__(cls) -> typing.Dict[str, typing.Any]:
        return {}

    id = mapped_column(Integer, primary_key=True)


class Foo(Base):
    __tablename__ = "foo"

    id = mapped_column(Integer, primary_key=True)


u1 = User()

if typing.TYPE_CHECKING:
    assert_type(User.__tablename__, str)

    assert_type(Foo.__tablename__, str)

    assert_type(u1.related_data, str)

    assert_type(User.related_data, InstrumentedAttribute[str])
