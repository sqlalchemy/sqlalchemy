"""this suite experiments with other kinds of relationship syntaxes.

"""
from __future__ import annotations

import typing
from typing import List
from typing import Optional
from typing import Set

from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()

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


if typing.TYPE_CHECKING:
    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[Union\[builtins.str, None\]\]
    reveal_type(User.extra)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[Union\[builtins.str, None\]\]
    reveal_type(User.extra_name)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[builtins.str\*?\]
    reveal_type(Address.email)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[builtins.str\*?\]
    reveal_type(Address.email_name)

    # EXPECTED_RE_TYPE: sqlalchemy.*.InstrumentedAttribute\[builtins.list\*?\[experimental_relationship.Address\]\]
    reveal_type(User.addresses_style_one)

    # EXPECTED_RE_TYPE: sqlalchemy.orm.attributes.InstrumentedAttribute\[builtins.set\*?\[experimental_relationship.Address\]\]
    reveal_type(User.addresses_style_two)
