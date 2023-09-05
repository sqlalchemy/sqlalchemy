"""Tests Mapped covariance."""

from typing import Protocol

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


# Protocols


class ParentProtocol(Protocol):
    name: Mapped[str]


class ChildProtocol(Protocol):
    # Read-only for simplicity, mutable protocol members are complicated,
    # see https://mypy.readthedocs.io/en/latest/common_issues.html#covariant-subtyping-of-mutable-protocol-members-is-rejected
    @property
    def parent(self) -> Mapped[ParentProtocol]:
        ...


def get_parent_name(child: ChildProtocol) -> str:
    return child.parent.name


# Implementations


class Base(DeclarativeBase):
    pass


class Parent(Base):
    __tablename__ = "parent"

    name: Mapped[str] = mapped_column(primary_key=True)


class Child(Base):
    __tablename__ = "child"

    name: Mapped[str] = mapped_column(primary_key=True)
    parent_name: Mapped[str] = mapped_column(ForeignKey(Parent.name))

    parent: Mapped[Parent] = relationship()


assert get_parent_name(Child(parent=Parent(name="foo"))) == "foo"
