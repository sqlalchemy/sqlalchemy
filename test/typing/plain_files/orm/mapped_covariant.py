"""Tests Mapped covariance."""

from datetime import datetime
from typing import List
from typing import Protocol
from typing import Sequence
from typing import TypeVar
from typing import Union

from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Nullable
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.elements import SQLCoreOperations

# Protocols


class ParentProtocol(Protocol):
    # Read-only for simplicity, mutable protocol members are complicated,
    # see https://mypy.readthedocs.io/en/latest/common_issues.html#covariant-subtyping-of-mutable-protocol-members-is-rejected
    @property
    def name(self) -> Mapped[str]: ...


class ChildProtocol(Protocol):
    # Read-only for simplicity, mutable protocol members are complicated,
    # see https://mypy.readthedocs.io/en/latest/common_issues.html#covariant-subtyping-of-mutable-protocol-members-is-rejected
    @property
    def parent(self) -> Mapped[ParentProtocol]: ...


def get_parent_name(child: ChildProtocol) -> str:
    return child.parent.name


# Implementations


class Base(DeclarativeBase):
    pass


class Parent(Base):
    __tablename__ = "parent"

    name: Mapped[str] = mapped_column(primary_key=True)

    children: Mapped[Sequence["Child"]] = relationship("Child")


class Child(Base):
    __tablename__ = "child"

    name: Mapped[str] = mapped_column(primary_key=True)
    parent_name: Mapped[str] = mapped_column(ForeignKey(Parent.name))

    parent: Mapped[Parent] = relationship()


assert get_parent_name(Child(parent=Parent(name="foo"))) == "foo"

# Make sure that relationships are covariant as well
_BaseT = TypeVar("_BaseT", bound=Base, covariant=True)
RelationshipType = Union[
    InstrumentedAttribute[_BaseT],
    InstrumentedAttribute[Sequence[_BaseT]],
    InstrumentedAttribute[Union[_BaseT, None]],
]


def operate_on_relationships(
    relationships: List[RelationshipType[_BaseT]],
) -> int:
    return len(relationships)


assert operate_on_relationships([Parent.children, Child.parent]) == 2

# other test


class NullableModel(DeclarativeBase):
    not_null: Mapped[datetime]
    nullable: Mapped[Union[datetime, None]]


test = NullableModel()
test.not_null = func.now()
test.nullable = func.now()

nullable_now: SQLCoreOperations[Union[datetime, None]] = Nullable(func.now())
test.nullable = Nullable(func.now())
