import typing
from typing import assert_type
from typing import Protocol
from uuid import UUID
from uuid import uuid4

from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy import UUID as Uuid
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class HasRelatedDataMixin:
    @declared_attr
    def related_data(cls) -> Mapped[str]:
        return mapped_column(Text(), deferred=True)


class HasIdProtocol(Protocol):
    @property
    def id(self) -> Mapped[int | UUID]: ...


def compare_id(
    left: HasIdProtocol,
    right: HasIdProtocol,
) -> bool:
    return left.id == right.id


class HasIntIdMixin:
    @declared_attr
    def id(cls) -> Mapped[int]:
        return mapped_column(Integer, primary_key=True)


class HasUuidIdMixin:
    @declared_attr
    def id(cls) -> Mapped[UUID]:
        return mapped_column(Uuid, primary_key=True, default=uuid4)


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


class IntIdModel(HasIntIdMixin, Base):
    __tablename__ = "int_id_model"


class UuidIdModel(HasUuidIdMixin, Base):
    __tablename__ = "uuid_id_model"


u1 = User()
id1 = IntIdModel()
id2 = UuidIdModel()


def _int_id(cls: type[object]) -> Mapped[int]:
    return mapped_column(Integer, primary_key=True)


int_id_attr: declared_attr[int] = declared_attr(_int_id)
union_id_attr: declared_attr[int | UUID] = int_id_attr
assert union_id_attr

if typing.TYPE_CHECKING:
    assert_type(User.__tablename__, str)

    assert_type(Foo.__tablename__, str)

    assert_type(u1.related_data, str)

    assert_type(User.related_data, InstrumentedAttribute[str])

    assert_type(compare_id(id1, id2), bool)
