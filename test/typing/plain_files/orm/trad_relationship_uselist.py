"""traditional relationship patterns with explicit uselist."""

import typing
from typing import cast
from typing import Dict
from typing import List
from typing import Set
from typing import Type

from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_keyed_dict


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String, nullable=False)

    addresses_style_one: Mapped[List["Address"]] = relationship(
        "Address", uselist=True
    )

    addresses_style_two: Mapped[Set["Address"]] = relationship(
        "Address", collection_class=set
    )

    addresses_style_three = relationship("Address", collection_class=set)

    addresses_style_three_cast = relationship(
        cast(Type["Address"], "Address"), collection_class=set
    )

    addresses_style_four = relationship("Address", collection_class=list)


class Address(Base):
    __tablename__ = "address"

    id = mapped_column(Integer, primary_key=True)
    user_id = mapped_column(ForeignKey("user.id"))
    email = mapped_column(String, nullable=False)

    user_style_one = relationship(User, uselist=False)

    user_style_one_typed: Mapped[User] = relationship(User, uselist=False)

    user_style_two = relationship("User", uselist=False)

    user_style_two_typed: Mapped["User"] = relationship("User", uselist=False)

    # these is obviously not correct relationally but want to see the typing
    # work out with a real class passed as the argument
    user_style_three: Mapped[List[User]] = relationship(User, uselist=True)

    user_style_four: Mapped[List[User]] = relationship("User", uselist=True)

    user_style_five: Mapped[List[User]] = relationship(User, uselist=True)

    user_style_six: Mapped[Set[User]] = relationship(
        User, uselist=True, collection_class=set
    )

    user_style_seven = relationship(User, uselist=True, collection_class=set)

    user_style_eight = relationship(User, uselist=True, collection_class=list)

    user_style_nine = relationship(User, uselist=True)

    user_style_ten = relationship(
        User, collection_class=attribute_keyed_dict("name")
    )

    user_style_ten_typed: Mapped[Dict[str, User]] = relationship(
        User, collection_class=attribute_keyed_dict("name")
    )

    # pylance rejects this however.  cannot get both to work at the same
    # time.
    # if collection_class is cast() to mutablemapping, then pylance seems
    # OK.  cannot make sense of the errors or what would the official way to
    # do these things would be.  pylance keeps changing and newly breaking
    # things, never know what's a bug, what's a "known limitation", and what's
    # "you need to learn more".   I can't imagine most programmers being able
    # to navigate this stuff
    # user_style_ten_typed_mapping: Mapped[MutableMapping[str, User]] = relationship(
    #      User, collection_class=attribute_mapped_collection("name")
    # )


if typing.TYPE_CHECKING:
    # EXPECTED_TYPE: InstrumentedAttribute[list[Address]]
    reveal_type(User.addresses_style_one)

    # EXPECTED_TYPE: InstrumentedAttribute[set[Address]]
    reveal_type(User.addresses_style_two)

    # EXPECTED_TYPE: InstrumentedAttribute[Any]
    reveal_type(User.addresses_style_three)

    # EXPECTED_TYPE: InstrumentedAttribute[Any]
    reveal_type(User.addresses_style_three_cast)

    # EXPECTED_TYPE: InstrumentedAttribute[Any]
    reveal_type(User.addresses_style_four)

    # EXPECTED_TYPE: InstrumentedAttribute[Any]
    reveal_type(Address.user_style_one)

    # EXPECTED_TYPE: InstrumentedAttribute[User]
    reveal_type(Address.user_style_one_typed)

    # EXPECTED_TYPE: InstrumentedAttribute[Any]
    reveal_type(Address.user_style_two)

    # EXPECTED_TYPE: InstrumentedAttribute[User]
    reveal_type(Address.user_style_two_typed)

    # reveal_type(Address.user_style_six)

    # reveal_type(Address.user_style_seven)

    # EXPECTED_TYPE: InstrumentedAttribute[Any]
    reveal_type(Address.user_style_eight)

    # EXPECTED_TYPE: InstrumentedAttribute[Any]
    reveal_type(Address.user_style_nine)

    # EXPECTED_TYPE: InstrumentedAttribute[Any]
    reveal_type(Address.user_style_ten)

    # EXPECTED_TYPE: InstrumentedAttribute[dict[str, User]]
    reveal_type(Address.user_style_ten_typed)
