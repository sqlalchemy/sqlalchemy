from typing import TYPE_CHECKING

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.orm import as_declarative
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from .address import Address
from .user import User

if TYPE_CHECKING:
    from sqlalchemy.orm.decl_api import DeclarativeMeta


@as_declarative()
class Base(object):
    @declared_attr
    def __tablename__(self) -> Mapped[str]:
        return self.__name__.lower()

    id = Column(Integer, primary_key=True)


__all__ = ["User", "Address"]
