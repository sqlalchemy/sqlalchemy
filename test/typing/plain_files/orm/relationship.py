"""this suite experiments with other kinds of relationship syntaxes."""

from __future__ import annotations

import typing
from typing import ClassVar
from typing import List
from typing import Optional
from typing import Set
from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy.orm import aliased
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import registry
from sqlalchemy.orm import Relationship
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import with_polymorphic


class Base(DeclarativeBase):
    pass


class Group(Base):
    __tablename__ = "group"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()

    addresses_style_one_anno_only: Mapped[List["User"]]
    addresses_style_two_anno_only: Mapped[Set["User"]]


class User(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    group_id = mapped_column(ForeignKey("group.id"))

    # this currently doesnt generate an error.  not sure how to get the
    # overloads to hit this one, nor am i sure i really want to do that
    # anyway
    name_this_works_atm: Mapped[str] = mapped_column(nullable=True)

    extra: Mapped[Optional[str]] = mapped_column()
    extra_name: Mapped[Optional[str]] = mapped_column("extra_name")

    addresses_style_one: Mapped[List["Address"]] = relationship()
    addresses_style_two: Mapped[Set["Address"]] = relationship()


class Address(Base):
    __tablename__ = "address"

    id = mapped_column(Integer, primary_key=True)
    user_id = mapped_column(ForeignKey("user.id"))
    email: Mapped[str]
    email_name: Mapped[str] = mapped_column("email_name")

    user_style_one: Mapped[User] = relationship()
    user_style_two: Mapped["User"] = relationship()

    rel_style_one: Relationship[List["MoreMail"]] = relationship()
    # everything works even if using Relationship instead of Mapped
    # users should use Mapped though
    rel_style_one_anno_only: Relationship[Set["MoreMail"]]


class MoreMail(Base):
    __tablename__ = "address"

    id = mapped_column(Integer, primary_key=True)
    aggress_id = mapped_column(ForeignKey("address.id"))
    email: Mapped[str]


class SelfReferential(Base):
    """test for #9150"""

    __tablename__ = "MyTable"

    idx: Mapped[int] = mapped_column(Integer, primary_key=True)
    mytable_id: Mapped[int] = mapped_column(ForeignKey("MyTable.idx"))

    not_anno = mapped_column(Integer)

    selfref_1: Mapped[Optional[SelfReferential]] = relationship(
        remote_side=idx
    )
    selfref_2: Mapped[Optional[SelfReferential]] = relationship(
        foreign_keys=mytable_id
    )

    selfref_3: Mapped[Optional[SelfReferential]] = relationship(
        remote_side=not_anno
    )


class Employee(Base):
    __tablename__ = "employee"
    id: Mapped[int] = mapped_column(primary_key=True)
    team_id: Mapped[int] = mapped_column(ForeignKey("team.id"))
    team: Mapped["Team"] = relationship(back_populates="employees")

    __mapper_args__ = {
        "polymorphic_on": "type",
        "polymorphic_identity": "employee",
    }


class Team(Base):
    __tablename__ = "team"
    id: Mapped[int] = mapped_column(primary_key=True)
    employees: Mapped[list[Employee]] = relationship("Employee")


class Engineer(Employee):
    engineer_info: Mapped[str]

    __mapper_args__ = {"polymorphic_identity": "engineer"}


if typing.TYPE_CHECKING:
    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[Union\[builtins.str, None\]\]
    reveal_type(User.extra)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[Union\[builtins.str, None\]\]
    reveal_type(User.extra_name)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[builtins.str\*?\]
    reveal_type(Address.email)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[builtins.str\*?\]
    reveal_type(Address.email_name)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[builtins.list\*?\[relationship.Address\]\]
    reveal_type(User.addresses_style_one)

    # EXPECTED_RE_TYPE: sqlalchemy.orm.attributes.InstrumentedAttribute\[builtins.set\*?\[relationship.Address\]\]
    reveal_type(User.addresses_style_two)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[builtins.list\*?\[relationship.User\]\]
    reveal_type(Group.addresses_style_one_anno_only)

    # EXPECTED_RE_TYPE: sqlalchemy.orm.attributes.InstrumentedAttribute\[builtins.set\*?\[relationship.User\]\]
    reveal_type(Group.addresses_style_two_anno_only)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[builtins.list\*?\[relationship.MoreMail\]\]
    reveal_type(Address.rel_style_one)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[builtins.set\*?\[relationship.MoreMail\]\]
    reveal_type(Address.rel_style_one_anno_only)

    # EXPECTED_RE_TYPE: sqlalchemy.*.QueryableAttribute\[relationship.Engineer\]
    reveal_type(Team.employees.of_type(Engineer))

    # EXPECTED_RE_TYPE: sqlalchemy.*.QueryableAttribute\[relationship.Employee\]
    reveal_type(Team.employees.of_type(aliased(Employee)))

    # EXPECTED_RE_TYPE: sqlalchemy.*.QueryableAttribute\[relationship.Engineer\]
    reveal_type(Team.employees.of_type(aliased(Engineer)))

    # EXPECTED_RE_TYPE: sqlalchemy.*.QueryableAttribute\[relationship.Employee\]
    reveal_type(Team.employees.of_type(with_polymorphic(Employee, [Engineer])))


mapper_registry: registry = registry()

e = create_engine("sqlite:///")


@mapper_registry.mapped
class A:
    __tablename__ = "a"
    id: Mapped[int] = mapped_column(primary_key=True)
    b_id: Mapped[int] = mapped_column(ForeignKey("b.id"))
    number: Mapped[int] = mapped_column(primary_key=True)
    number2: Mapped[int] = mapped_column(primary_key=True)
    if TYPE_CHECKING:
        __table__: ClassVar[Table]


@mapper_registry.mapped
class B:
    __tablename__ = "b"
    id: Mapped[int] = mapped_column(primary_key=True)

    # Omit order_by
    a1: Mapped[list[A]] = relationship("A", uselist=True)

    # All kinds of order_by
    a2: Mapped[list[A]] = relationship(
        "A", uselist=True, order_by=(A.id, A.number)
    )
    a3: Mapped[list[A]] = relationship(
        "A", uselist=True, order_by=[A.id, A.number]
    )
    a4: Mapped[list[A]] = relationship("A", uselist=True, order_by=A.id)
    a5: Mapped[list[A]] = relationship(
        "A", uselist=True, order_by=A.__table__.c.id
    )
    a6: Mapped[list[A]] = relationship("A", uselist=True, order_by="A.number")

    # Same kinds but lambda'd
    a7: Mapped[list[A]] = relationship(
        "A", uselist=True, order_by=lambda: (A.id, A.number)
    )
    a8: Mapped[list[A]] = relationship(
        "A", uselist=True, order_by=lambda: [A.id, A.number]
    )
    a9: Mapped[list[A]] = relationship(
        "A", uselist=True, order_by=lambda: A.id
    )


mapper_registry.metadata.drop_all(e)
mapper_registry.metadata.create_all(e)

with Session(e) as s:
    s.execute(select(B).options(joinedload(B.a1)))
    s.execute(select(B).options(joinedload(B.a2)))
    s.execute(select(B).options(joinedload(B.a3)))
    s.execute(select(B).options(joinedload(B.a4)))
    s.execute(select(B).options(joinedload(B.a5)))
    s.execute(select(B).options(joinedload(B.a7)))
    s.execute(select(B).options(joinedload(B.a8)))
    s.execute(select(B).options(joinedload(B.a9)))
