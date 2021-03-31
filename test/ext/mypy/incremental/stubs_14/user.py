from typing import List
from typing import TYPE_CHECKING

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm.decl_api import declared_attr
from sqlalchemy.orm.relationships import RelationshipProperty
from . import Base

if TYPE_CHECKING:
    from .address import Address


class User(Base):
    name = Column(String)

    othername = Column(String)

    addresses: Mapped[List["Address"]] = relationship(
        "Address", back_populates="user"
    )


class HasUser:
    @declared_attr
    def user_id(self) -> "Column[Integer]":
        return Column(
            Integer,
            ForeignKey(User.id, ondelete="CASCADE", onupdate="CASCADE"),
            nullable=False,
        )

    @declared_attr
    def user(self) -> RelationshipProperty[User]:
        return relationship(User)
