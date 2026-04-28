from typing import Protocol
from typing import assert_type
from uuid import UUID
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped


class ModelBase(DeclarativeBase):
    pass


class CompareProtocol(Protocol):
    @property
    def id(self) -> Mapped[int | UUID]: ...


class CompareMixin:
    def compare(self: CompareProtocol, other: CompareProtocol) -> bool:
        return self.id == other.id


class IntIdMixin:
    @declared_attr
    def id(cls) -> Mapped[int]:
        return sa.orm.mapped_column(sa.Integer, primary_key=True)


class UuidIdMixin:
    @declared_attr
    def id(cls) -> Mapped[UUID]:
        return sa.orm.mapped_column(sa.UUID, primary_key=True, default=uuid4)


class MyModel(CompareMixin, IntIdMixin, ModelBase):
    __tablename__ = "my_model"


class MyUuidModel(CompareMixin, UuidIdMixin, ModelBase):
    __tablename__ = "my_uuid_model"


m1 = MyModel()
m2 = MyModel()
u1 = MyUuidModel()


def _int_id(cls: type[object]) -> Mapped[int]:
    return sa.orm.mapped_column(sa.Integer, primary_key=True)


int_id_attr: declared_attr[int] = declared_attr(_int_id)
union_id_attr: declared_attr[int | UUID] = int_id_attr
assert union_id_attr

assert_type(m1.compare(m2), bool)
assert_type(m1.compare(u1), bool)
