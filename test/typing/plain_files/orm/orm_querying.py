from __future__ import annotations

from sqlalchemy import ColumnElement
from sqlalchemy import ForeignKey
from sqlalchemy import orm
from sqlalchemy import ScalarSelect
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
    a: Mapped[A] = relationship()


def test_9669_and() -> None:
    select(A).options(selectinload(A.bs.and_(B.data == "some data")))


def test_9669_of_type() -> None:
    ba = aliased(B)
    select(A).options(selectinload(A.bs.of_type(ba)))


def load_options_ok() -> None:
    select(B).options(
        orm.contains_eager("*").contains_eager(A.bs),
        orm.load_only("*").load_only(A.bs),
        orm.joinedload("*").joinedload(A.bs),
        orm.subqueryload("*").subqueryload(A.bs),
        orm.selectinload("*").selectinload(A.bs),
        orm.lazyload("*").lazyload(A.bs),
        orm.immediateload("*").immediateload(A.bs),
        orm.noload("*").noload(A.bs),
        orm.raiseload("*").raiseload(A.bs),
        orm.defaultload("*").defaultload(A.bs),
        orm.defer("*").defer(A.bs),
        orm.undefer("*").undefer(A.bs),
    )
    select(B).options(
        orm.contains_eager(B.a).contains_eager("*"),
        orm.load_only(B.a).load_only("*"),
        orm.joinedload(B.a).joinedload("*"),
        orm.subqueryload(B.a).subqueryload("*"),
        orm.selectinload(B.a).selectinload("*"),
        orm.lazyload(B.a).lazyload("*"),
        orm.immediateload(B.a).immediateload("*"),
        orm.noload(B.a).noload("*"),
        orm.raiseload(B.a).raiseload("*"),
        orm.defaultload(B.a).defaultload("*"),
        orm.defer(B.a).defer("*"),
        orm.undefer(B.a).undefer("*"),
    )


def load_options_error() -> None:
    select(B).options(
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.contains_eager("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.load_only("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.joinedload("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.subqueryload("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.selectinload("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.lazyload("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.immediateload("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.noload("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.raiseload("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.defaultload("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.defer("foo"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.undefer("foo"),
    )
    select(B).options(
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.contains_eager(B.a).contains_eager("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.load_only(B.a).load_only("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.joinedload(B.a).joinedload("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.subqueryload(B.a).subqueryload("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.selectinload(B.a).selectinload("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.lazyload(B.a).lazyload("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.immediateload(B.a).immediateload("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.noload(B.a).noload("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.raiseload(B.a).raiseload("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.defaultload(B.a).defaultload("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.defer(B.a).defer("bar"),
        # EXPECTED_MYPY_RE: Argument 1 to .* has incompatible type .*
        orm.undefer(B.a).undefer("bar"),
    )


# test 10959
def test_10959_with_loader_criteria() -> None:
    def where_criteria(cls_: type[A]) -> ColumnElement[bool]:
        return cls_.data == "some data"

    orm.with_loader_criteria(A, lambda cls: cls.data == "some data")
    orm.with_loader_criteria(A, where_criteria)


def test_10937() -> None:
    stmt: ScalarSelect[bool] = select(A.id == B.id).scalar_subquery()
    stmt1: ScalarSelect[bool] = select(A.id > 0).scalar_subquery()
    stmt2: ScalarSelect[int] = select(A.id + 2).scalar_subquery()
    stmt3: ScalarSelect[str] = select(A.data + B.data).scalar_subquery()

    select(stmt, stmt2, stmt3, stmt1)


def test_bundles() -> None:
    b1 = orm.Bundle("b1", A.id, A.data)
    orm.Bundle("b2", A.id, A.data, b1)
