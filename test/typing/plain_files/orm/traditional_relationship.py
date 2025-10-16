"""Here we illustrate 'traditional' relationship that looks as much like
1.x SQLAlchemy as possible.   We want to illustrate that users can apply
Mapped[...] on the left hand side and that this will work in all cases.
This requires that the return type of relationship is based on Any,
if no uselists are present.

"""

import typing
from typing import Any
from typing import assert_type
from typing import List
from typing import Set

from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String, nullable=False)

    addresses_style_one: Mapped[List["Address"]] = relationship("Address")

    addresses_style_two: Mapped[Set["Address"]] = relationship(
        "Address", collection_class=set
    )


class Address(Base):
    __tablename__ = "address"

    id = mapped_column(Integer, primary_key=True)
    user_id = mapped_column(ForeignKey("user.id"))
    email = mapped_column(String, nullable=False)

    user_style_one = relationship(User)

    user_style_one_typed: Mapped[User] = relationship(User)

    user_style_two = relationship("User")

    user_style_two_typed: Mapped["User"] = relationship("User")

    # this is obviously not correct relationally but want to see the typing
    # work out
    user_style_three: Mapped[List[User]] = relationship(User)

    user_style_four: Mapped[List[User]] = relationship("User")

    user_style_five = relationship(User, collection_class=set)

    user_fk_style_one: Mapped[List[User]] = relationship(
        foreign_keys="Address.user_id"
    )
    user_fk_style_two: Mapped[List[User]] = relationship(
        foreign_keys=lambda: Address.user_id
    )
    user_fk_style_three: Mapped[List[User]] = relationship(
        foreign_keys=[user_id]
    )
    user_pj_style_one: Mapped[List[User]] = relationship(
        primaryjoin=user_id == User.id
    )
    user_pj_style_two: Mapped[List[User]] = relationship(
        primaryjoin=lambda: Address.user_id == User.id
    )
    user_pj_style_three: Mapped[List[User]] = relationship(
        primaryjoin="Address.user_id == User.id"
    )


if typing.TYPE_CHECKING:
    assert_type(User.addresses_style_one, InstrumentedAttribute[list[Address]])

    assert_type(User.addresses_style_two, InstrumentedAttribute[set[Address]])

    assert_type(Address.user_style_one, InstrumentedAttribute[Any])

    assert_type(Address.user_style_one_typed, InstrumentedAttribute[User])

    assert_type(Address.user_style_two, InstrumentedAttribute[Any])

    assert_type(Address.user_style_two_typed, InstrumentedAttribute[User])

    assert_type(Address.user_style_three, InstrumentedAttribute[list[User]])

    assert_type(Address.user_style_four, InstrumentedAttribute[list[User]])

    assert_type(Address.user_style_five, InstrumentedAttribute[Any])
