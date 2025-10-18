from __future__ import annotations

import typing
from typing import assert_type

from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm import WriteOnlyMapped
from sqlalchemy.orm.writeonly import WriteOnlyCollection


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
    addresses: WriteOnlyMapped[Address] = relationship()


with Session() as session:
    u = User()
    session.add(u)
    session.commit()

    if typing.TYPE_CHECKING:
        assert_type(u.addresses, WriteOnlyCollection[Address])

    address = session.scalars(
        u.addresses.select().filter(Address.email_address.like("xyz"))
    ).one()

    if typing.TYPE_CHECKING:
        assert_type(address, Address)

    u.addresses.add(Address())
    u.addresses.add_all([Address(), Address()])

    # this should emit an error, because __iter__ is NoReturn,
    # however typing tools don't pick up on that right now
    current_addresses = list(u.addresses)

    u.addresses.add(Address())

    session.commit()

    # test #9985
    stmt = select(User).join(User.addresses)
