from __future__ import annotations

import typing
from typing import assert_type

from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import DynamicMapped
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.dynamic import AppenderQuery


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
        assert_type(u.addresses, AppenderQuery[Address])

    count = u.addresses.count()
    if typing.TYPE_CHECKING:
        assert_type(count, int)

    address = u.addresses.filter(Address.email_address.like("xyz")).one()

    if typing.TYPE_CHECKING:
        assert_type(address, Address)

    u.addresses.append(Address())
    u.addresses.extend([Address(), Address()])

    current_addresses = list(u.addresses)

    if typing.TYPE_CHECKING:
        assert_type(current_addresses, list[Address])

    # can assign plain list
    u.addresses = []

    # or anything
    u.addresses = set()

    if typing.TYPE_CHECKING:
        # still an AppenderQuery
        assert_type(u.addresses, AppenderQuery[Address])

    u.addresses = {Address(), Address()}

    if typing.TYPE_CHECKING:
        # still an AppenderQuery
        assert_type(u.addresses, AppenderQuery[Address])

    u.addresses.append(Address())

    session.commit()

    # test #9985
    stmt = select(User).join(User.addresses)
    
    # test #13128
    if typing.TYPE_CHECKING:
        assert_type(u.addresses[0], Address)
        assert_type(u.addresses[1:3], list[Address])
