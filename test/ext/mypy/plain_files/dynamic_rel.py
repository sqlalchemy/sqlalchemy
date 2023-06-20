from __future__ import annotations

import typing

from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import DynamicMapped
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


class Base(DeclarativeBase):
    pass


class Address(Base):
    __tablename__ = "address"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    email_address: Mapped[str]


class User(Base):
    __tablename__ = "user"
    id: Mapped[int] = mapped_column(primary_key=True)
    addresses: DynamicMapped[Address] = relationship(
        cascade="all,delete-orphan"
    )


with Session() as session:
    u = User()
    session.add(u)
    session.commit()

    if typing.TYPE_CHECKING:
        # EXPECTED_TYPE: AppenderQuery[Address]
        reveal_type(u.addresses)

    count = u.addresses.count()
    if typing.TYPE_CHECKING:
        # EXPECTED_TYPE: int
        reveal_type(count)

    address = u.addresses.filter(Address.email_address.like("xyz")).one()

    if typing.TYPE_CHECKING:
        # EXPECTED_TYPE: Address
        reveal_type(address)

    u.addresses.append(Address())
    u.addresses.extend([Address(), Address()])

    current_addresses = list(u.addresses)

    if typing.TYPE_CHECKING:
        # EXPECTED_TYPE: list[Address]
        reveal_type(current_addresses)

    # can assign plain list
    u.addresses = []

    # or anything
    u.addresses = set()

    if typing.TYPE_CHECKING:
        # still an AppenderQuery
        # EXPECTED_TYPE: AppenderQuery[Address]
        reveal_type(u.addresses)

    u.addresses = {Address(), Address()}

    if typing.TYPE_CHECKING:
        # still an AppenderQuery
        # EXPECTED_TYPE: AppenderQuery[Address]
        reveal_type(u.addresses)

    u.addresses.append(Address())

    session.commit()

    # test #9985
    stmt = select(User).join(User.addresses)
