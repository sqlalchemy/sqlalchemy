from typing import List
from typing import Optional

from sqlalchemy import Column
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from sqlalchemy.orm.decl_api import declared_attr
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import Integer
from sqlalchemy.sql.sqltypes import String

reg: registry = registry()


@reg.mapped
class User:
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    name = Column(String(50))

    name3 = Column(String(50))

    addresses: List["Address"] = relationship("Address")


@reg.mapped
class SubUser(User):
    __tablename__ = "subuser"

    id: int = Column(ForeignKey("user.id"), primary_key=True)

    @declared_attr
    def name(cls) -> Column[String]:
        return Column(String(50))

    @declared_attr
    def name2(cls) -> Mapped[Optional[str]]:
        return Column(String(50))

    @declared_attr
    def name3(cls) -> Mapped[str]:
        return Column(String(50))

    subname = Column(String)


@reg.mapped
class Address:
    __tablename__ = "address"

    id = Column(Integer, primary_key=True)
    user_id: int = Column(ForeignKey("user.id"))
    email = Column(String(50))

    user = relationship(User, uselist=False)


s1 = SubUser()

# EXPECTED_MYPY: Incompatible types in assignment (expression has type "Optional[str]", variable has type "str" # noqa
x1: str = s1.name

# EXPECTED_MYPY: Incompatible types in assignment (expression has type "Optional[str]", variable has type "str") # noqa
x2: str = s1.name2

x3: str = s1.name3

u1 = User()

# EXPECTED_MYPY: Incompatible types in assignment (expression has type "Optional[str]", variable has type "str") # noqa
x4: str = u1.name3
