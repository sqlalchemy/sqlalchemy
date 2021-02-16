"""Test that the right-hand expressions we normally "replace" are actually
type checked.

"""
from typing import List

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import relationship
from sqlalchemy.orm.decl_api import declared_attr


Base = declarative_base()


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)

    # EXPECTED_MYPY: Unexpected keyword argument "wrong_arg" for "RelationshipProperty" # noqa
    addresses: Mapped[List["Address"]] = relationship(
        "Address", wrong_arg="imwrong"
    )


class SubUser(User):
    __tablename__ = "subuser"

    id: int = Column(Integer, ForeignKey("user.id"), primary_key=True)


class Address(Base):
    __tablename__ = "address"

    id: int = Column(Integer, primary_key=True)

    user_id: int = Column(ForeignKey("user.id"))

    @declared_attr
    def email_address(cls) -> Column[String]:
        # EXPECTED_MYPY: No overload variant of "Column" matches argument type "bool" # noqa
        return Column(True)

    @declared_attr
    # EXPECTED_MYPY: Invalid type comment or annotation
    def thisisweird(cls) -> Column(String):
        # with the bad annotation mypy seems to not go into the
        # function body
        return Column(False)
