import typing
from typing import assert_type
from typing import Set

from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.ext.associationproxy import AssociationProxyInstance
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String, nullable=False)

    addresses: Mapped[Set["Address"]] = relationship()

    email_addresses: AssociationProxy[Set[str]] = association_proxy(
        "addresses", "email"
    )


class Address(Base):
    __tablename__ = "address"

    id = mapped_column(Integer, primary_key=True)
    user_id = mapped_column(ForeignKey("user.id"))
    email = mapped_column(String, nullable=False)


u1 = User()

if typing.TYPE_CHECKING:
    assert_type(User.email_addresses, AssociationProxyInstance[set[str]])

    assert_type(u1.email_addresses, set[str])
